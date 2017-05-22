from datetime import datetime, timezone, timedelta
import calendar, time, os, json

def resultToFiles(drd, file_dir, data_name, time_freq):
    out = {}

    f_index = file_dir + data_name + '-index.json'
    f_index_change = False
    ws_index = {}

    if (time_freq == '12h'):
        freq = timedelta(hours=12)
    elif (time_freq == '1h'):
        freq = timedelta(hours=1)
    elif (time_freq == '10m'):
        freq = timedelta(minutes=10)
    else:
        freq = timedelta()

    if (os.path.isfile(f_index)):
        with open(f_index, 'r') as f:
             ws_index = json.load(f)

    for x in drd.collect():
        ts = int(time.mktime(x[0][1].timetuple()))
        if (ts not in out):
            out[ts] = []
        out[ts].append((x[0][0], x[1]))

    for x in out:
        f = file_dir + data_name + '.txt-' + str(x)

        if (os.path.isfile(f)):
            os.remove(f)

        target_file = os.open(f, os.O_RDWR|os.O_CREAT)
        os.write(target_file, str.encode(json.dumps(out[x])))
        os.close(target_file)

        if (x not in ws_index):
            f_index_change = True

            tstime = datetime.fromtimestamp(x)

            drange_low = tstime.strftime('%d-%b-%Y %I:%M%p')
            drange_top = (tstime + freq).strftime('%d-%b-%Y %I:%M%p')
            drange = drange_low + ' - ' + drange_top

            ws_index[x] = {'ts': x, 'file': data_name + '.txt-' + str(x), 'date_range': drange, 'drange_low': drange_low, 'drange_top': drange_top}


    if (f_index_change):
        if (os.path.isfile(f_index)):
            os.remove(f_index)

        f = open(f_index, 'w+')
        f.write(json.dumps(ws_index))
        f.close()

def reflect(tuple, up_hours):
    out = []
    for i in range(0, up_hours):
        out.append(((tuple[0][0], tuple[0][1] + timedelta(hours=i)), tuple[1]))
    return out
