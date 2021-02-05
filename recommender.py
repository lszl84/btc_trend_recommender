import binance.client
import twilio.rest
import pandas as pd
import numpy as np
import configparser
import datetime
import time
import os


def download_current_ticks(client, product, gran, past_window_width):
    if gran == 3600*4:
        interval = binance.client.Client.KLINE_INTERVAL_4HOUR
    elif gran == 3600:
        interval = binance.client.Client.KLINE_INTERVAL_1HOUR
    elif gran == 900:
        interval = binance.client.Client.KLINE_INTERVAL_15MINUTE
    elif gran == 300:
        interval = binance.client.Client.KLINE_INTERVAL_5MINUTE
    elif gran == 60:
        interval = binance.client.Client.KLINE_INTERVAL_1MINUTE

    required_second = 0

    now_datetime = datetime.datetime.now()

    if now_datetime.minute % int(gran/60) != 0 or now_datetime.second > required_second:
        m_delta = int(gran/60) - (now_datetime.minute % int(gran/60))
        next_datetime = now_datetime + datetime.timedelta(minutes=m_delta)

        next_datetime = next_datetime.replace(
            second=required_second, microsecond=0)

        import pause
        pause.until(next_datetime)
        now_datetime = next_datetime

    # downloading more in case there are missing candles
    time_start_iter = now_datetime - \
        datetime.timedelta(minutes=(past_window_width*2)*gran/60)
    # make sure we don't download the started candlestick
    time_end_iter = (now_datetime - datetime.timedelta(minutes=1)
                     ).replace(second=59, microsecond=999999)

    iso_now = time_end_iter.astimezone(tz=datetime.timezone.utc).isoformat()
    iso_prev = time_start_iter.astimezone(tz=datetime.timezone.utc).isoformat()

    olhc = client.get_historical_klines(
        product, interval, iso_prev, iso_now)

    rates = [(str(datetime.datetime.fromtimestamp(time/1000)), low, high, open,
              close, volume) for (time, open, high, low, close, volume, _, _, _, _, _, _) in olhc]

    df = pd.DataFrame(rates)
    df.columns = ["date", "low", "high", "open", "close", "volume"]
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index(pd.DatetimeIndex(df['date']))
    df.pop('date')

    df = df.astype(float)
    df.sort_index(inplace=True)

    return df


def setup_rolling_windows(df, x):
    trigger_window = int(x[0])
    stop_window = int(x[1])
    volume_window = int(x[2])
    volume_threshold = x[3]

    df['long_entry'] = df['close'].rolling(
        window=trigger_window).max().shift(1)
    df['long_exit'] = df['close'].rolling(window=stop_window).mean().shift(1)

    df['short_entry'] = df['close'].rolling(
        window=trigger_window).min().shift(1)
    df['short_exit'] = df['high'].shift(1) #df['close'].rolling(window=stop_window).mean().shift(1)

    df['volume_average'] = df['volume'].rolling(
        window=volume_window).mean().shift(1)

    df['volume_threshold'] = volume_threshold

    return df


def setup_twil_client():
    config = configparser.ConfigParser()
    config.read('secret-twilio.txt')

    config = config['Twilio']

    client = twilio.rest.Client(config['account_sid'], config['auth_token'])

    return client, config['source_phone'], config['destination_phone']


def setup_binance_client():
    config = configparser.ConfigParser()
    config.read('secret-binance-prod.txt')

    config = config['Binance']
    client = binance.client.Client(config['api_key'], config['api_secret'])
    return client


product = "BTCUSDT"

try:
    binance_client = setup_binance_client()
    twilio_client, source_phone, destination_phone = setup_twil_client()
except:
    print(f'Twilio and Binance config files required.')
    exit(1)

gran = 3600
fee = 0.00075
funding_fee = 0.0008

# params = [46.388868,  19.00115881, 6.98667037, 1.43094999]
params = [46, 19, 7, 0]
window_width = max(params) * 3

it = 0
in_position = False
long_position = False

starting_capital = 6400
capital_usdt = starting_capital
risk_fraction = 0.04
leverage = 10

while True:

    df = download_current_ticks(binance_client, product, gran, window_width)
    df = setup_rolling_windows(df, params)

    last_tick = df.iloc[-1]

    # we do not want to compound, but if we lose money, we cannot trade with the capital we do not have!
    # risk = risk_fraction * min(capital_usdt, starting_capital)
    risk = risk_fraction * capital_usdt
    # buying_power = leverage * min(capital_usdt, starting_capital)
    buying_power = leverage * capital_usdt


    msg = None

    if not in_position:

        if capital_usdt <= 0:
            break

        print(
            f"No position. Last close: {last_tick['close']:.3f}. Long entry > {last_tick['long_entry']:.3f}. Short entry < {last_tick['short_entry']:.3f}", end='\r')

        if last_tick['volume'] < last_tick['volume_average'] * last_tick['volume_threshold']:
            continue

        if last_tick['close'] > last_tick['long_entry']:
            current_stop = last_tick['long_exit']
            initial_stop = current_stop

            buy_price = last_tick['close']
            stop_size = buy_price - current_stop

            stop_size /= 2
            current_stop += stop_size

            if stop_size <= 10 or stop_size >= 700:  # we need decent stop size to account for fees
                continue

            in_position = True
            long_position = True

            # position_size_nofee = risk / stop_size
            # position_size = risk/(stop_size + buy_price *
            #                       funding_fee + 2*buy_price*fee)

            # print("KACZKA", position_size, position_size_nofee)

            R = 700/stop_size

            position_size = risk / stop_size
            position_size = min(buying_power / buy_price, position_size)

            target = buy_price + R * stop_size

            msg = f'Open LONG {position_size:.6f} BTC ({(position_size*buy_price):.2f} USD) at price ${buy_price:.3f}. Stop loss at ${current_stop:.3f} ({current_stop/buy_price*100.0:.2f}%, target at ${target:.2f}, size={stop_size:.3f})'

        elif last_tick['close'] < last_tick['short_entry']:

            current_stop = last_tick['short_exit']
            initial_stop = current_stop

            sell_price = last_tick['close']
            stop_size = current_stop - sell_price

            # stop_size /= 2
            # current_stop -= stop_size

            if stop_size <= 10 or stop_size >= 700:
                continue

            in_position = True
            long_position = False

            # position_size_nofee = risk / stop_size
            # position_size = risk/(stop_size + sell_price *
            #                       funding_fee + 2*sell_price*fee)

            # print("KACZKA", position_size, position_size_nofee)

            # if position_size < 0.0001:
            #     continue

            position_size = risk / stop_size
            position_size = min(buying_power / sell_price, position_size)

            R = 700/stop_size

            target = sell_price - R * stop_size

            msg = f'Open SHORT {position_size:.6f} BTC ({(position_size*sell_price):.2f} USD) at price ${sell_price:.3f}. Stop loss at ${current_stop:.3f} ({current_stop/sell_price*100.0:.2f}%, target at ${target:.2f}, size={stop_size:.3f})'

    else:

        if long_position:
            last_stop = current_stop
            # change stop only if we are not stopped out by current stop!
            # AND the next stop does not stop us out immediately LOL
            # if last_tick['low'] > current_stop and last_tick['low'] > last_tick['long_exit']:
            #     current_stop = max(current_stop, last_tick['long_exit'])

            # if current_stop > last_stop:
            #     msg = f' -- LONG RAISE STOP to ${current_stop:.3f} ({current_stop/buy_price*100.0:.2f}%, size={stop_size:.3f})'

            if last_tick['low'] <= current_stop:
                in_position = False

                sell_price = current_stop
                result = position_size * sell_price * \
                    (1.0 - fee) - position_size * buy_price * \
                    (1.0 + fee) - position_size * sell_price * funding_fee

                capital_usdt += result
                msg = f' * [stp] CLOSED long position (${buy_price:.3f}) at ${sell_price:.3f} for PnL={result:.3f}. Capital = ${capital_usdt:.3f}'
            elif last_tick['high'] >= target:
                in_position = False

                sell_price = target
                result = position_size * sell_price * \
                    (1.0 - fee) - position_size * buy_price * \
                    (1.0 + fee) - position_size * sell_price * funding_fee

                capital_usdt += result
                msg = f' * [tgt] CLOSED long position (${buy_price:.3f}) at ${sell_price:.3f} for PnL={result:.3f}. Capital = ${capital_usdt:.3f}'
            else:
                print(
                    f"Long position {position_size:.6f} BTC @ {buy_price:.3f}. Last close: {last_tick['close']:.3f}. Current stop: {current_stop:.3f}", end='\r')
        else:
            last_stop = current_stop
            # change stop only if we are not stopped out by current stop!
            # if last_tick['high'] < current_stop and last_tick['high'] < last_tick['short_exit']:
            #     current_stop = min(current_stop, last_tick['short_exit'])

            # if current_stop < last_stop:
            #     msg = f' -- SHORT LOWER STOP to ${current_stop:.3f} ({current_stop/sell_price*100.0:.2f}%, size={stop_size:.3f})'

            if last_tick['high'] >= current_stop:
                in_position = False

                buy_price = current_stop
                result = position_size * sell_price * \
                    (1.0 - fee) - position_size * buy_price * \
                    (1.0 + fee) - position_size * sell_price * funding_fee

                capital_usdt += result

                msg = f' * [stp] CLOSED short position (${sell_price:.3f}) at ${buy_price:.3f} for PnL={result:.3f}. Capital = ${capital_usdt:.3f}'
            elif last_tick['low'] <= target:
                in_position = False

                buy_price = target
                result = position_size * sell_price * \
                    (1.0 - fee) - position_size * buy_price * \
                    (1.0 + fee) - position_size * sell_price * funding_fee

                capital_usdt += result

                msg = f' * [tgt] CLOSED short position (${sell_price:.3f}) at ${buy_price:.3f} for PnL={result:.3f}. Capital = ${capital_usdt:.3f}'
            else:
                print(
                    f"Short position {position_size:.6f} BTC @ {sell_price:.3f}. Last close: {last_tick['close']:.3f}. Current stop: {current_stop:.3f}", end='\r')

    if msg is not None:
        print(f"[{last_tick.name}] {msg}")
        twilio_client.messages.create(
            body=msg,
            from_=source_phone,
            to=destination_phone)
