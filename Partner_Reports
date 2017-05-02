
#!/usr/bin/python
 
##################
#Scrip to automate horizon's enrollments and the bucket link for document uploads
 
import datetime
import os
import MySQLdb
import csv
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import encoders
 
#Helper Objects
db_username=os.getenv('REPORTING_DATABASE_USERNAME')
db_password=os.getenv('REPORTING_DATABASE_PASSWORD')
 
SSL = {"cert": "W:\\certs\\Norvax_MySQL.crt",
        "key": "W:\\certs\\Norvax_MySQL.key",
        "ca": "W:\\certs\\Norvax_MySQL_CA.crt"}
 
db_host="db-reporting-slave.wip-prod.norvax.net"
db="eapp"
 
file_name="Horizon_Bucket_ids_" + str(datetime.date.today())+".csv"
 
 
##email configuration here
fromaddr = ""
toaddr = ""
mail_server = "mail.gohealth.com"
port = 25
 
horizon_query="""SELECT
    IFNULL(pu.email, pu.username) as 'email',
    eai.firstName AS 'consumer_fn',
    eai.lastName AS 'consumer_ln',
    ebi.firstName AS 'agent_fn',
    ebi.lastName AS 'agent_ln',
    CONCAT('https://shop.horizonblue.com/upload-documents/?bucketId=',ed.bucketId) AS 'bucket_url',
    ear.submittedDate AS submitted_date,
    ppe.name
FROM
    eapp.document ed
        JOIN
    eapp.applicant_response ear ON ear.id = ed.applicantResponse_id
        JOIN
    eapp.applicant_information eai ON eai.id = ear.applicantInformation_id
        JOIN
    eapp.broker_information ebi ON ebi.id = ear.brokerInformation_id
        JOIN
    eapp.applicant ea ON ea.id = ear.applicant_id
        JOIN
    phx.off_exchange_enrollment poee ON(poee.eapp_id=ear.externalId)
        JOIN
    phx.marketplace_profile pmp ON(pmp.id=poee.profile_id)
        JOIN
    phx.user pu ON(pu.id=pmp.user_id)
        JOIN
    phx.private_exchange ppe ON(ppe.id=pu.exchange_id)
WHERE
    ppe.name='horizon'
AND
        ear.submittedDate >= NOW() - INTERVAL 7 DAY ORDER BY ear.submittedDate DESC;
"""
#Database Operations
def database_operation(db_host, db_username, db_password, ssl, db, horizon_query):
 
    def __init__(db_host, db_username, db_password, SSL, db, horizon_query):
        this.db_host = db_host
        this.db_username = db_username
        this.db_password = db_password
        this.SSL= SSL
        this.db = db
        this.horizon_query = horizon_query
 
    conn = MySQLdb.connect(db_host, user=db_username, passwd=db_password, ssl=SSL, db=db)
    cur = conn.cursor()
 
    query = "SELECT * from eapp.applicant_information WHERE lastName='DeStefano' AND firstName='Brian';"
    cur.execute(horizon_query)
    results_handler = cur.fetchall()
    query_results = results_handler
    conn.close()
    return query_results
     
 
#Write out to a csv file
def write_to_csv(file_name, query_results):
 
    def __init__(file_name, query_results):
        this.filename = filename
        this.query_results = query_results
 
    with open(file_name, 'wb') as csvfile:
        fieldnames = ['email', 'first_name', 'last_name', 'agent_fname', 'agent_lname', 'bucket_url', 'submitted_date', 'carrier']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for x in query_results:
            writer.writerow({'email':x[0], 'first_name':x[1], 'last_name':x[2], 'agent_fname':x[3], 'agent_lname':x[4], 'bucket_url':x[5], 'submitted_date':x[6], 'carrier':x[7]})
    return file_name
 
 
def send_email(fromaddr, toaddr, mail_server, port, horizon_file, file_name):
 
    def __init__(fromaddr, toaddr, mail_server, port, horizon_file):
        this.fromaddr = fromaddr
        this.toaddr = toaddr
        this.mail_server = mail_server
        this.port = port
        this.horizon_file = horizon_file
        this.file_name = file_name
 
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Horizon Buckets"
    body = ""
 
    msg.attach(MIMEText(body, 'plain'))
    attachment = open(horizon_file, "rb")
 
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename=%s' % file_name)
 
    msg.attach(part)
 
    try:
        send_email=smtplib.SMTP(mail_server, port)
        send_email.starttls()
        send_email.sendmail(fromaddr, toaddr, msg.as_string())
        send_email.quit()
    except Exception as e:
        raise e
 
if __name__ == '__main__':
    try:
        results = database_operation(db_host, db_username, db_password, SSL, db, horizon_query)
        horizon_file = write_to_csv(file_name, results)
        send_email(fromaddr, toaddr, mail_server, port, horizon_file, file_name)
    except:
        print("this script has failed")
