from flask import Flask, request, send_file, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename
import os
import sqlite3
from datetime import datetime, timedelta
import random
import logging

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_migration.log'),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)

# Get the absolute path to the directory containing this script
basedir = os.path.abspath(os.path.dirname(__file__))

# Configure the SQLAlchemy database URI
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{os.path.join(basedir, "new_database.db")}'
app.config['UPLOAD_FOLDER'] = os.path.join(basedir, 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max-limit

db = SQLAlchemy(app)

# Ensure the upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


# Model definitions
class Staff(db.Model):
    __tablename__ = 'staff'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    leave_days_remaining = db.Column(db.Float, default=0.0)
    is_team_leader = db.Column(db.Boolean, default=False)
    receive_notifications = db.Column(db.Boolean, default=True)
    engagements = db.relationship('Engagement', backref='team_leader', lazy=True)
    proposals = db.relationship('Proposal', backref='team_leader', lazy=True)
    hours_logs = db.relationship('HoursLog', backref='staff_member', lazy=True)
    leave_records = db.relationship('LeaveRecord', backref='staff', lazy=True)
    utilizations = db.relationship('Utilization', backref='staff', lazy=True)


class Engagement(db.Model):
    __tablename__ = 'engagement'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    team_leader_id = db.Column(db.Integer, db.ForeignKey('staff.id'))
    status = db.Column(db.String(10), nullable=False)


class Proposal(db.Model):
    __tablename__ = 'proposal'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    due_date = db.Column(db.Date)
    team_leader_id = db.Column(db.Integer, db.ForeignKey('staff.id'))
    status = db.Column(db.String(10), nullable=False)


class NonBillable(db.Model):
    __tablename__ = 'non_billable'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)


class HoursLog(db.Model):
    __tablename__ = 'hours_log'
    id = db.Column(db.Integer, primary_key=True)
    staff_id = db.Column(db.Integer, db.ForeignKey('staff.id'))
    category = db.Column(db.String(20), nullable=False)
    item_id = db.Column(db.Integer, nullable=False)
    hours = db.Column(db.Float, nullable=False)
    date = db.Column(db.Date, nullable=False)


class LeaveRecord(db.Model):
    __tablename__ = 'leave_record'
    id = db.Column(db.Integer, primary_key=True)
    staff_id = db.Column(db.Integer, db.ForeignKey('staff.id'))
    date = db.Column(db.Date, nullable=False)
    __table_args__ = (db.UniqueConstraint('staff_id', 'date', name='_staff_date_uc'),)


class Utilization(db.Model):
    __tablename__ = 'utilization'
    id = db.Column(db.Integer, primary_key=True)
    staff_id = db.Column(db.Integer, db.ForeignKey('staff.id'))
    week_start = db.Column(db.Date, nullable=False)
    client_utilization_year_to_date = db.Column(db.Float, default=0.0)
    client_utilization_month_to_date = db.Column(db.Float, default=0.0)
    resource_utilization_year_to_date = db.Column(db.Float, default=0.0)
    resource_utilization_month_to_date = db.Column(db.Float, default=0.0)


def convert_date(date_str):
    if isinstance(date_str, str):
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    return date_str


def fake_description():
    return "This is a sample description generated during data migration."


def fake_date():
    return datetime.now().date() + timedelta(days=random.randint(1, 365))


def transfer_data(old_db_path):
    logging.info(f"Starting data transfer from {old_db_path}")

    if not os.path.exists(old_db_path):
        logging.error(f"Old database not found at {old_db_path}")
        return False

    # Connect to the old database
    old_conn = sqlite3.connect(old_db_path)
    old_cursor = old_conn.cursor()

    try:
        # Start a transaction
        db.session.begin_nested()

        # Clear existing data from all tables
        logging.info("Clearing existing data from all tables")
        NonBillable.query.delete()
        HoursLog.query.delete()
        LeaveRecord.query.delete()
        Utilization.query.delete()
        Proposal.query.delete()
        Engagement.query.delete()
        Staff.query.delete()

        # Transfer Staff data
        logging.info("Transferring Staff data")
        old_cursor.execute("SELECT * FROM staff")
        staff_data = old_cursor.fetchall()
        for row in staff_data:
            staff = Staff(
                id=row[0],
                name=row[1],
                email=row[2],
                leave_days_remaining=float(row[3]),
                is_team_leader=bool(row[4]),
                receive_notifications=bool(row[5])
            )
            db.session.add(staff)
        logging.info(f"Transferred {len(staff_data)} Staff records")

        # Commit staff data first and flush the session
        db.session.flush()

        # Transfer Engagement data
        logging.info("Transferring Engagement data")
        old_cursor.execute("SELECT * FROM engagement")
        engagement_data = old_cursor.fetchall()
        for row in engagement_data:
            engagement = Engagement(
                id=row[0],
                name=row[1],
                team_leader_id=row[2],
                status=row[3],
                description=fake_description(),
                start_date=fake_date(),
                end_date=fake_date()
            )
            db.session.add(engagement)
        logging.info(f"Transferred {len(engagement_data)} Engagement records")

        # Transfer Proposal data
        logging.info("Transferring Proposal data")
        old_cursor.execute("SELECT * FROM proposal")
        proposal_data = old_cursor.fetchall()
        for row in proposal_data:
            proposal = Proposal(
                id=row[0],
                name=row[1],
                team_leader_id=row[2],
                status=row[3],
                description=fake_description(),
                due_date=fake_date()
            )
            db.session.add(proposal)
        logging.info(f"Transferred {len(proposal_data)} Proposal records")

        # Transfer NonBillable data
        logging.info("Transferring NonBillable data")
        old_cursor.execute("SELECT * FROM non_billable")
        non_billable_data = old_cursor.fetchall()
        for row in non_billable_data:
            non_billable = NonBillable(
                id=row[0],
                name=row[1]
            )
            db.session.add(non_billable)
        logging.info(f"Transferred {len(non_billable_data)} NonBillable records")

        # Transfer HoursLog data
        logging.info("Transferring HoursLog data")
        old_cursor.execute("SELECT * FROM hours_log")
        hours_log_data = old_cursor.fetchall()
        for row in hours_log_data:
            hours_log = HoursLog(
                id=row[0],
                staff_id=row[1],
                category=row[2],
                item_id=row[3],
                hours=float(row[4]),
                date=convert_date(row[5])
            )
            db.session.add(hours_log)
        logging.info(f"Transferred {len(hours_log_data)} HoursLog records")

        # Transfer LeaveRecord data
        logging.info("Transferring LeaveRecord data")
        old_cursor.execute("SELECT * FROM leave_record")
        leave_record_data = old_cursor.fetchall()
        leave_records_added = 0
        for row in leave_record_data:
            try:
                leave_record = LeaveRecord(
                    id=row[0],
                    staff_id=row[1],
                    date=convert_date(row[2])
                )
                db.session.add(leave_record)
                leave_records_added += 1
            except IntegrityError:
                logging.warning(f"Skipping duplicate leave record for staff {row[1]} on {row[2]}")
                db.session.rollback()
                continue
        logging.info(f"Transferred {leave_records_added} LeaveRecord records")

        # Transfer Utilization data
        logging.info("Transferring Utilization data")
        old_cursor.execute("SELECT * FROM utilization")
        utilization_data = old_cursor.fetchall()
        for row in utilization_data:
            utilization = Utilization(
                id=row[0],
                staff_id=row[1],
                week_start=convert_date(row[2]),
                client_utilization_year_to_date=float(row[3]),
                client_utilization_month_to_date=float(row[4]),
                resource_utilization_year_to_date=float(row[5]),
                resource_utilization_month_to_date=float(row[6])
            )
            db.session.add(utilization)
        logging.info(f"Transferred {len(utilization_data)} Utilization records")

        # Commit all changes
        db.session.commit()
        logging.info("Data transfer completed successfully")
        return True

    except sqlite3.OperationalError as e:
        logging.error(f"SQLite Operational Error: {e}")
        db.session.rollback()
        return False
    except IntegrityError as e:
        logging.error(f"Integrity Error: {e}")
        db.session.rollback()
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        db.session.rollback()
        return False
    finally:
        old_conn.close()


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file part'
        file = request.files['file']
        if file.filename == '':
            return 'No selected file'
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            success = transfer_data(file_path)

            if success:
                return send_file(
                    os.path.join(basedir, 'new_database.db'),
                    as_attachment=True,
                    download_name='new_database.db'
                )
            else:
                return 'Error occurred during data transfer'
    return render_template('upload.html')


def create_database():
    """Create the new database and all tables"""
    with app.app_context():
        db.create_all()
        logging.info("Database and tables created successfully")


if __name__ == '__main__':
    # Create the database and tables
    create_database()

    # Run the Flask application
    app.run(debug=True)