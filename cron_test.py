from datetime import datetime
f = open("cron_test.log", "a")
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
write_string = "cron ran at: {0}\n".format(dt_string)
f.write(write_string)
f.close()



