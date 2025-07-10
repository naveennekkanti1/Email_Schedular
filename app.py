from flask import Flask, render_template, request, flash, redirect, url_for, jsonify
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from werkzeug.utils import secure_filename
import re
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from pymongo import MongoClient
from bson import ObjectId
import uuid
import threading
import logging
from pytz import timezone
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# SMTP Configuration
SMTP_CONFIG = {
    'host': 'smtp.gmail.com',
    'port': 587,
    'email': 'srmcorporationservices@gmail.com',
    'password': 'bxxo qcvd njfj kcsa',
    'use_tls': True
}

# MongoDB Setup with error handling
try:
    mongo_client = MongoClient(
        'mongodb+srv://durganaveen:nekkanti@cluster0.8nibi9x.mongodb.net/PHOTOREVIVE?retryWrites=true&w=majority&appName=Cluster0',
        serverSelectionTimeoutMS=5000
    )
    mongo_client.admin.command('ping')  # test connection
    mongo_db = mongo_client['email_scheduler']
    email_jobs_collection = mongo_db['email_jobs']
    logger.info("MongoDB connected successfully")
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    mongo_client = None
    mongo_db = None
    email_jobs_collection = None

# File upload config
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Scheduler config
executors = {'default': ThreadPoolExecutor(20)}
job_defaults = {'coalesce': False, 'max_instances': 3}
scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)

scheduled_jobs = {}
job_lock = threading.Lock()

# Load existing jobs
def load_scheduled_jobs():
    if email_jobs_collection is None:
        logger.warning("MongoDB not available, skipping job loading")
        return
    try:
        jobs = email_jobs_collection.find({'status': 'scheduled'})
        for job in jobs:
            try:
                schedule_dt = job['schedule_time']
                if isinstance(schedule_dt, str):
                    schedule_dt = datetime.fromisoformat(schedule_dt.replace('Z', '+00:00'))
                if schedule_dt > datetime.now():
                    scheduler.add_job(
                        func=send_bulk_emails_task,
                        args=[str(job['_id']), job['emails'], job['subject'], job['message']],
                        trigger='date',
                        run_date=schedule_dt,
                        id=str(job['_id']),
                        replace_existing=True
                    )
                    with job_lock:
                        scheduled_jobs[str(job['_id'])] = {
                            'emails': job['emails'],
                            'subject': job['subject'],
                            'message': job['message'],
                            'schedule_time': schedule_dt,
                            'status': 'scheduled'
                        }
                    logger.info(f"Loaded scheduled job: {job['_id']}")
                else:
                    email_jobs_collection.update_one(
                        {'_id': job['_id']},
                        {'$set': {'status': 'missed'}}
                    )
            except Exception as e:
                logger.error(f"Error loading job {job['_id']}: {e}")
    except Exception as e:
        logger.error(f"Error loading scheduled jobs: {e}")

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def validate_email(email):
    return re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email) is not None

def extract_emails_from_file(file_path):
    try:
        df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)

        print("DataFrame loaded:")
        print(df.head())  # Debugging: show data

        emails = []

        # Try to locate a column that looks like it contains emails
        email_col = None
        for col in df.columns:
            if 'email' in col.lower():
                email_col = col
                break

        if not email_col:
            logger.warning("No column with 'email' in its name found.")
            return []

        column_emails = df[email_col].dropna().tolist()

        for email in column_emails:
            email_str = str(email).strip()
            if validate_email(email_str):
                emails.append(email_str)

        unique_emails = list(set(emails))
        print("Extracted emails:")
        print(unique_emails)
        return unique_emails

    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return []

# Background email task
def send_bulk_emails_task(job_id, emails, subject, message):
    logger.info(f"Starting email task for job {job_id}")
    try:
        with job_lock:
            if job_id in scheduled_jobs:
                scheduled_jobs[job_id]['status'] = 'sending'

        if email_jobs_collection is not None:
            email_jobs_collection.update_one(
                {'_id': ObjectId(job_id)},
                {'$set': {'status': 'sending', 'started_at': datetime.utcnow()}}
            )

        server = smtplib.SMTP(SMTP_CONFIG['host'], SMTP_CONFIG['port'])
        if SMTP_CONFIG['use_tls']:
            server.starttls()
        server.login(SMTP_CONFIG['email'], SMTP_CONFIG['password'])

        sent_count = 0
        failed_emails = []

        for email in emails:
            try:
                msg = MIMEMultipart()
                msg['From'] = SMTP_CONFIG['email']
                msg['To'] = email
                msg['Subject'] = subject
                msg.attach(MIMEText(message, 'plain'))
                server.send_message(msg)
                sent_count += 1
                logger.info(f"Email sent to {email}")
            except Exception as e:
                failed_emails.append(email)
                logger.error(f"Failed to send to {email}: {e}")

        server.quit()

        if email_jobs_collection is not None:
            email_jobs_collection.update_one(
                {'_id': ObjectId(job_id)},
                {'$set': {
                    'status': 'completed',
                    'completed_at': datetime.utcnow(),
                    'sent_count': sent_count,
                    'failed_count': len(failed_emails),
                    'failed_emails': failed_emails
                }}
            )

        with job_lock:
            if job_id in scheduled_jobs:
                scheduled_jobs[job_id]['status'] = 'completed'
                scheduled_jobs[job_id]['sent_count'] = sent_count
                scheduled_jobs[job_id]['failed_count'] = len(failed_emails)

    except Exception as e:
        logger.error(f"Error in send_bulk_emails_task for job {job_id}: {e}")
        with job_lock:
            if job_id in scheduled_jobs:
                scheduled_jobs[job_id]['status'] = 'failed'
        if email_jobs_collection is not None:
            email_jobs_collection.update_one(
                {'_id': ObjectId(job_id)},
                {'$set': {
                    'status': 'failed',
                    'error': str(e),
                    'failed_at': datetime.utcnow()
                }}
            )

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files or request.files['file'].filename == '':
        return jsonify({'error': 'No file selected'})
    file = request.files['file']
    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            emails = extract_emails_from_file(filepath)
            os.remove(filepath)
            return jsonify({'success': True, 'emails': emails}) if emails else jsonify({'error': 'No valid emails found in file'})
        except Exception as e:
            logger.error(f"Error processing uploaded file: {e}")
            return jsonify({'error': 'Error processing file'})
    return jsonify({'error': 'Invalid file format. Please upload CSV or XLSX files.'})

@app.route('/send_emails', methods=['POST'])
def send_emails():
    try:
        data = request.json
        emails = data.get('emails', [])
        if not emails and data.get('manual_emails'):
            emails = [e.strip() for e in data['manual_emails'].split(',') if e.strip()]
        valid_emails = [e for e in emails if validate_email(e)]
        if not valid_emails:
            return jsonify({'error': 'No valid emails provided'})

        subject, message, schedule_time = data.get('subject', '').strip(), data.get('message', '').strip(), data.get('schedule_time', '').strip()
        if not subject or not message:
            return jsonify({'error': 'Subject and message are required'})

        job_id = str(ObjectId())
        job_data = {
            '_id': ObjectId(job_id),
            'emails': valid_emails,
            'subject': subject,
            'message': message,
            'schedule_time': schedule_time if schedule_time else None,
            'status': 'scheduled' if schedule_time else 'pending',
            'created_at': datetime.utcnow(),
            'email_count': len(valid_emails)
        }

        if email_jobs_collection is not None:
            email_jobs_collection.insert_one(job_data)

        if schedule_time:
            schedule_dt = datetime.fromisoformat(schedule_time.replace('Z', '+00:00'))
            if schedule_dt <= datetime.now():
                return jsonify({'error': 'Schedule time must be in the future'})
            scheduler.add_job(
                func=send_bulk_emails_task,
                args=[job_id, valid_emails, subject, message],
                trigger='date',
                run_date=schedule_dt,
                id=job_id,
                replace_existing=True
            )
            with job_lock:
                scheduled_jobs[job_id] = {
                    'emails': valid_emails,
                    'subject': subject,
                    'message': message,
                    'schedule_time': schedule_dt,
                    'status': 'scheduled'
                }
            return jsonify({'success': True, 'job_id': job_id, 'scheduled': True, 'schedule_time': schedule_dt.isoformat()})
        else:
            if email_jobs_collection is not None:
                email_jobs_collection.update_one({'_id': ObjectId(job_id)}, {'$set': {'status': 'sending'}})
            threading.Thread(target=send_bulk_emails_task, args=(job_id, valid_emails, subject, message), daemon=True).start()
            return jsonify({'success': True, 'job_id': job_id, 'scheduled': False})
    except Exception as e:
        logger.error(f"Error in send_emails: {e}")
        return jsonify({'error': 'Internal server error'})

@app.route('/job_status/<job_id>')
def job_status(job_id):
    try:
        with job_lock:
            job_info = scheduled_jobs.get(job_id)
        if job_info:
            return jsonify(job_info)
        if email_jobs_collection is not None:
            job = email_jobs_collection.find_one({'_id': ObjectId(job_id)})
            if job:
                job['_id'] = str(job['_id'])
                return jsonify(job)
        return jsonify({'status': 'not found'})
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        return jsonify({'error': 'Error retrieving job status'})

@app.route('/scheduled_jobs')
def scheduled_jobs_list():
    try:
        jobs = []
        with job_lock:
            for job_id, info in scheduled_jobs.items():
                jobs.append({
                    'id': job_id,
                    'subject': info['subject'],
                    'email_count': len(info['emails']),
                    'schedule_time': info['schedule_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    'status': info['status']
                })
        if email_jobs_collection is not None:
            for job in email_jobs_collection.find({'status': {'$in': ['completed', 'failed', 'cancelled']}}).sort('created_at', -1).limit(20):
                job_info = {
                    'id': str(job['_id']),
                    'subject': job['subject'],
                    'email_count': job['email_count'],
                    'schedule_time': job.get('schedule_time', job['created_at']).strftime('%Y-%m-%d %H:%M:%S'),
                    'status': job['status']
                }
                if not any(j['id'] == job_info['id'] for j in jobs):
                    jobs.append(job_info)
        return jsonify(jobs)
    except Exception as e:
        logger.error(f"Error getting scheduled jobs: {e}")
        return jsonify({'error': 'Error retrieving scheduled jobs'})

@app.route('/cancel_job/<job_id>', methods=['POST'])
def cancel_job(job_id):
    try:
        try:
            scheduler.remove_job(job_id)
        except:
            pass
        with job_lock:
            if job_id in scheduled_jobs:
                scheduled_jobs[job_id]['status'] = 'cancelled'
        if email_jobs_collection is not None:
            email_jobs_collection.update_one(
                {'_id': ObjectId(job_id)},
                {'$set': {'status': 'cancelled', 'cancelled_at': datetime.utcnow()}}
            )
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        return jsonify({'error': str(e)})

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'mongodb': 'connected' if email_jobs_collection is not None else 'disconnected',
        'scheduler': 'running' if scheduler.running else 'stopped',
        'scheduled_jobs_count': len(scheduled_jobs)
    })

if __name__ == '__main__':
    try:
        load_scheduled_jobs()
        if not scheduler.running:
            scheduler.start()
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Error starting application: {e}")
    finally:
        if scheduler.running:
            scheduler.shutdown()
