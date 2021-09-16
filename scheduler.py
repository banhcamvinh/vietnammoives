import pytz
import requests
import subprocess
from apscheduler.schedulers.twisted import TwistedScheduler
from twisted.internet import reactor

def send_request():
    requests.post("https://vietnammovies.herokuapp.com/schedule.json", data={
        "project": "vietnammovies",
        "spider": "movies"
    })

if __name__ == "__main__":
    subprocess.run("scrapyd-deploy", shell=True, universal_newlines=True)
    scheduler = TwistedScheduler(timezone=pytz.timezone('Asia/Saigon'))
    # cron trigger that schedules job every every 20 minutues on weekdays
    scheduler.add_job(send_request, 'cron', day_of_week='mon-fri',hour='*/24')
    # start the scheduler
    scheduler.start()
    reactor.run()