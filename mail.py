#coding: utf-8

import smtplib
from email.mime.text import MIMEText
from util.const import SENDER,PASS

def send(receiver, title, body):
    while True:
        try:
            host = 'smtp.163.com'
            port = 25
            sender = SENDER
            pwd = PASS

            msg = MIMEText(body, 'html')
            msg['subject'] = title
            msg['from'] = sender
            msg['to'] = receiver

            s = smtplib.SMTP(host, port)
            s.login(sender, pwd)
            s.sendmail(sender, receiver, msg.as_string())

            print 'The mail named %s to %s is sended successly.' % (title, receiver)
            break
        except:
            print "wrong"
