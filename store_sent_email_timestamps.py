import glob
import email, email.parser, email.utils
import datetime
import dateutil.parser
import os.path
import httplib2
import json


settings = json.load(open("settings.json"))

def send_data(username, data):
    http = httplib2.Http()
    resp, content = http.request(settings["server-url"], "POST", body=json.dumps(data))
    if int(resp.get("status", "500")) != 200:
        return False
    return True

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

c = 0
for user_folder in glob.glob(settings["imap-user-folder-pattern"]):
    if not os.path.isdir(user_folder):
        continue
    username = os.path.basename(user_folder).replace("__", "@")
    timestamps = set()
    email_timestamps = []
    last_update_file = user_folder+"/sent_timestamps-last_update"
    if os.path.exists(last_update_file):
        last_update = open(last_update_file).read()
    else:
        last_update = "0"

    for filename in glob.glob(user_folder+settings["imap-sent-folder-pattern"]):
        timestamp = os.path.basename(filename).split("_")[0]
        if timestamp < last_update:
            continue
        timestamps.add(timestamp)
        try:
            message = email.message_from_file(open(filename))
        except (IOError, EOFError):
            continue

        date = message.get("Date")
        if not date:
            continue
        from_addr = message.get("From")
        if not from_addr:
            continue
        from_addr = from_addr.split("<")
        if len(from_addr) != 2:
            continue
        from_addr = from_addr[1].replace(">", "").split("@")

        if from_addr[0].lower() != username.split("@")[0] or message.get("precedence") == "bulk" or message.get("x-autoreply") == "yes" or message.get("auto-submitted") == "auto-replied":
            c += 1
            continue
        parsed = dateutil.parser.parse(date)
        offset_str = str(parsed).rsplit("+", 1)
        pr = "+"
        if len(offset_str) == 1:
           offset_str = offset_str[0].rsplit("-", 1)
           pr = "-"
        if len(offset_str) == 2:
           offset_str = pr + offset_str[1]
        else:
           offset_str = ""
        if hasattr(parsed.tzinfo, "_offset"):
           offset = parsed.tzinfo._offset
        else:
           offset = datetime.timedelta(0)
        parsed = parsed.replace(tzinfo=None) - offset
        email_timestamps.append((os.path.basename(filename), str(parsed), offset_str))


    post_data = [{"tzinfo": offset_str, "is_utc": True, "system": "email_sent", "timestamp": item, "username": username, "data": filename} for filename, item, offset_str in email_timestamps]
    success_all = True
    for chunk in chunks(post_data, 50):
        success = send_data(username, chunk)
        if not success:
            success = send_data(username, chunk)
            if not success:
                success_all = False
    if not success_all:
        continue

    a = list(timestamps)
    if len(a) == 0:
        continue
    a.sort()
    last_timestamp = a[-1]
    open(last_update_file, "w").write(last_timestamp)
