from flask import Flask, render_template, request, flash, redirect, url_for, jsonify
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
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
import json

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
    email_templates_collection = mongo_db['email_templates']
    logger.info("MongoDB connected successfully")
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    mongo_client = None
    mongo_db = None
    email_jobs_collection = None
    email_templates_collection = None

# File upload config
UPLOAD_FOLDER = 'uploads'
ATTACHMENTS_FOLDER = 'attachments'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['ATTACHMENTS_FOLDER'] = ATTACHMENTS_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(ATTACHMENTS_FOLDER, exist_ok=True)

# Scheduler config
executors = {'default': ThreadPoolExecutor(20)}
job_defaults = {'coalesce': False, 'max_instances': 3}
scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)

scheduled_jobs = {}
job_lock = threading.Lock()

# Email Templates
DEFAULT_TEMPLATES = [
    {
        'name': 'Welcome Email',
        'subject': 'Welcome to Our Service!',
        'content': '''Dear {{name}},

Welcome to our service! We're excited to have you on board.

Here are your next steps:
1. Complete your profile
2. Explore our features
3. Contact us if you need help

Best regards,
The Team'''
    },
    {
        'name': 'Newsletter',
        'subject': 'Monthly Newsletter - {{month}}',
        'content': '''Hello {{name}},

Here's what's new this month:

🔥 New Features:
- Feature 1
- Feature 2
- Feature 3

📈 Company Updates:
- Update 1
- Update 2

Thanks for being part of our community!

Best regards,
Newsletter Team'''
    },
    {
        'name': 'Promotion',
        'subject': 'Special Offer Just for You!',
        'content': '''Hi {{name}},

We have a special offer just for you!

🎉 {{discount}}% OFF on all products
🕒 Valid until {{expiry_date}}
🎁 Use code: {{promo_code}}

Don't miss out on this amazing deal!

Shop now: {{website_url}}

Best regards,
Sales Team'''
    },
    {
        'name': 'Event Invitation',
        'subject': 'You\'re Invited: {{event_name}}',
        'content': '''Dear {{name}},

You're cordially invited to {{event_name}}!

📅 Date: {{event_date}}
🕒 Time: {{event_time}}
📍 Location: {{event_location}}

Please RSVP by {{rsvp_date}}.

We look forward to seeing you there!

Best regards,
Event Team'''
    }
]

# Initialize templates in database
def init_templates():
    if email_templates_collection is None:
        return
    
    try:
        # Check if templates exist
        if email_templates_collection.count_documents({}) == 0:
            email_templates_collection.insert_many(DEFAULT_TEMPLATES)
            logger.info("Default templates initialized")
    except Exception as e:
        logger.error(f"Error initializing templates: {e}")

# Load existing jobs
def load_scheduled_jobs():
    if email_jobs_collection is None:
        logger.warning("MongoDB not available, skipping job loading")
        return
    try:
        jobs = email_jobs_collection.find({'status': 'scheduled'})
        for job in jobs:
            try:
                schedule_dt = job['schedule_datetime']
                if isinstance(schedule_dt, str):
                    schedule_dt = datetime.fromisoformat(schedule_dt.replace('Z', '+00:00'))
                if schedule_dt > datetime.now():
                    scheduler.add_job(
                        func=send_bulk_emails_task,
                        args=[str(job['_id']), job['emails'], job['subject'], job['message'], job.get('attachments', [])],
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
                            'attachments': job.get('attachments', []),
                            'schedule_datetime': schedule_dt,
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

def allowed_image_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS

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

def replace_template_variables(content, variables):
    """Replace template variables in content with actual values"""
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"
        content = content.replace(placeholder, str(value))
    return content

# Background email task
def send_bulk_emails_task(job_id, emails, subject, message, attachments=None):
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
                
                # Add text content
                msg.attach(MIMEText(message, 'plain'))
                
                # Add attachments if any
                if attachments:
                    for attachment_path in attachments:
                        if os.path.exists(attachment_path):
                            with open(attachment_path, "rb") as attachment:
                                part = MIMEBase('application', 'octet-stream')
                                part.set_payload(attachment.read())
                                encoders.encode_base64(part)
                                part.add_header(
                                    'Content-Disposition',
                                    f'attachment; filename= {os.path.basename(attachment_path)}'
                                )
                                msg.attach(part)
                
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

        # Clean up attachment files
        if attachments:
            for attachment_path in attachments:
                try:
                    if os.path.exists(attachment_path):
                        os.remove(attachment_path)
                except Exception as e:
                    logger.error(f"Error removing attachment {attachment_path}: {e}")

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

@app.route('/upload_images', methods=['POST'])
def upload_images():
    if 'images' not in request.files:
        return jsonify({'error': 'No images uploaded'})
    
    files = request.files.getlist('images')
    uploaded_files = []
    
    for file in files:
        if file and file.filename != '' and allowed_image_file(file.filename):
            try:
                filename = secure_filename(file.filename)
                # Add timestamp to avoid conflicts
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_")
                filename = timestamp + filename
                filepath = os.path.join(app.config['ATTACHMENTS_FOLDER'], filename)
                file.save(filepath)
                uploaded_files.append({
                    'filename': filename,
                    'original_name': file.filename,
                    'path': filepath
                })
            except Exception as e:
                logger.error(f"Error saving image {file.filename}: {e}")
                return jsonify({'error': f'Error saving image {file.filename}'})
        else:
            return jsonify({'error': f'Invalid file format for {file.filename}. Please upload image files only.'})
    
    return jsonify({'success': True, 'files': uploaded_files})

@app.route('/get_templates')
def get_templates():
    if email_templates_collection is None:
        return jsonify({'templates': DEFAULT_TEMPLATES})
    
    try:
        templates = list(email_templates_collection.find({}, {'_id': 0}))
        return jsonify({'templates': templates})
    except Exception as e:
        logger.error(f"Error getting templates: {e}")
        return jsonify({'templates': DEFAULT_TEMPLATES})

@app.route('/save_template', methods=['POST'])
def save_template():
    if email_templates_collection is None:
        return jsonify({'error': 'Database not available'})
    
    try:
        data = request.json
        template = {
            'name': data.get('name', '').strip(),
            'subject': data.get('subject', '').strip(),
            'content': data.get('content', '').strip()
        }
        
        if not template['name'] or not template['subject'] or not template['content']:
            return jsonify({'error': 'All fields are required'})
        
        # Check if template already exists
        existing = email_templates_collection.find_one({'name': template['name']})
        if existing:
            email_templates_collection.update_one(
                {'name': template['name']},
                {'$set': template}
            )
        else:
            email_templates_collection.insert_one(template)
        
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error saving template: {e}")
        return jsonify({'error': 'Error saving template'})

@app.route('/delete_template', methods=['POST'])
def delete_template():
    if email_templates_collection is None:
        return jsonify({'error': 'Database not available'})
    
    try:
        data = request.json
        template_name = data.get('name', '').strip()
        
        if not template_name:
            return jsonify({'error': 'Template name is required'})
        
        result = email_templates_collection.delete_one({'name': template_name})
        if result.deleted_count > 0:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Template not found'})
    except Exception as e:
        logger.error(f"Error deleting template: {e}")
        return jsonify({'error': 'Error deleting template'})

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

        subject = data.get('subject', '').strip()
        message = data.get('message', '').strip()
        schedule_datetime = data.get('schedule_datetime', '').strip()
        attachments = data.get('attachments', [])
        template_variables = data.get('template_variables', {})

        if not subject or not message:
            return jsonify({'error': 'Subject and message are required'})

        # Replace template variables if provided
        if template_variables:
            subject = replace_template_variables(subject, template_variables)
            message = replace_template_variables(message, template_variables)

        # Process attachments
        attachment_paths = []
        for attachment in attachments:
            attachment_path = os.path.join(app.config['ATTACHMENTS_FOLDER'], attachment['filename'])
            if os.path.exists(attachment_path):
                attachment_paths.append(attachment_path)

        job_id = str(ObjectId())
        job_data = {
            '_id': ObjectId(job_id),
            'emails': valid_emails,
            'subject': subject,
            'message': message,
            'attachments': attachment_paths,
            'schedule_datetime':schedule_datetime,
            'status': 'scheduled' if schedule_datetime else 'pending',
            'created_at': datetime.utcnow(),
            'email_count': len(valid_emails)
        }

        # Handle scheduling
        if schedule_datetime:
            try:
                schedule_dt = datetime.fromisoformat(schedule_datetime.replace('Z', '+00:00'))
                job_data['schedule_datetime'] = schedule_dt

                if schedule_dt > datetime.utcnow():
                    scheduler.add_job(
                        func=send_bulk_emails_task,
                        args=[job_id, valid_emails, subject, message, attachment_paths],
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
                            'attachments': attachment_paths,
                            'schedule_datetime': schedule_dt,
                            'status': 'scheduled'
                        }
                else:
                    return jsonify({'error': 'Schedule time must be in the future'})
            except Exception as e:
                logger.error(f"Invalid schedule_datetime: {e}")
                return jsonify({'error': 'Invalid schedule_datetime format. Use ISO format.'})
        else:
            # Run immediately
            scheduler.add_job(
                func=send_bulk_emails_task,
                args=[job_id, valid_emails, subject, message, attachment_paths],
                trigger='date',
                run_date=datetime.utcnow(),
                id=job_id
            )

        if email_jobs_collection is not None:
            email_jobs_collection.insert_one(job_data)

        return jsonify({'success': True, 'job_id': job_id})

    except Exception as e:
        logger.error(f"Error in send_emails route: {e}")
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

@app.route('/get_scheduled_jobs')
def get_scheduled_jobs():
    if email_jobs_collection is None:
        return jsonify([])
    
    try:
        jobs = list(email_jobs_collection.find({"status": {"$in": ["scheduled", "completed"]}}))
        
        for job in jobs:
            job['id'] = str(job['_id'])
            job.pop('_id', None)
        
        return jsonify(jobs)
    except Exception as e:
        logger.error(f"Error getting scheduled jobs: {e}")
        return jsonify([])

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
                    'schedule_datetime': info['schedule_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                    'status': info['status']
                })
        if email_jobs_collection is not None:
            for job in email_jobs_collection.find({'status': {'$in': ['completed', 'failed', 'cancelled']}}).sort('created_at', -1).limit(20):
                job_info = {
                    'id': str(job['_id']),
                    'subject': job['subject'],
                    'email_count': job['email_count'],
                    'schedule_datetime': job.get('schedule_datetime', job['created_at']).strftime('%Y-%m-%d %H:%M:%S'),
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
        init_templates()
        load_scheduled_jobs()
        if not scheduler.running:
            scheduler.start()
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Error starting application: {e}")
    finally:
        if scheduler.running:
            scheduler.shutdown()
