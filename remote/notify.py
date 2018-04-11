# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need

# Open a plain text file for reading.  For this example, assume that
# the text file contains only ASCII characters.

# me == the sender's email address
# you == the recipient's email address

class GMail_Client:
    def __init__(self, user, password):
        self.user = user
        self.server = smtplib.SMTP('smtp.gmail.com:587')
        self.server.ehlo()
        self.server.starttls()
        self.server.login(user, password)

    def __enter__(self):
        pass

    def send(self, to, msg):
        msg = 'From: {}\r\nTo: {}\r\nSubject: {}\r\n\r\n{}'.format(self.user, to, 'test', msg)
        self.server.sendmail(self.user, to, msg)

    def __exit__(self, cls, value, traceback):
        self.server.close()


# Send the message via our own SMTP server.
