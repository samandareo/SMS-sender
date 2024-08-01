from flask import Flask, request, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler
import json
import db
import generatorSMS

app = Flask(__name__)

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(generatorSMS.generate_token, 'interval', minutes=20)
scheduler.start()

@app.route('/text')
def text():
    return render_template('change_text.html')

@app.route('/import')
def import_from_sheets():
    if db.import_thread is not None and db.import_thread.is_alive():
        return render_template('import.html', status="Status: Importing...")
    return render_template('import.html')

@app.route('/stop', methods=['POST'])
def stop_import():
    try:
        db.stop_import()
    except Exception as e:
        print(e)

    if db.import_thread is None:
        return render_template('import.html', status="Imported is stopped")
    else:
        return render_template('import.html', status=f"Importing is not stopped...")
    
@app.route('/start', methods=['POST'])
def start_import():
    start_error = "Not yet started"
    try:
        db.start_import()
    except Exception as e:
        start_error = e

    if db.import_thread is not None and db.import_thread.is_alive():
        return render_template('import.html', status="Importing is started...")
    else:
        return render_template('import.html', status=f"Import is not started. Error: {start_error}")


    
@app.route('/change_text', methods=['POST'])
def change_text():
    user_text = request.form.get('user_text')
    if user_text:
        with open('extras/sms_text.json', 'w') as f:
            json.dump({'text': user_text}, f)

        return f'Text changed to: {user_text}'
    else:
        return 'No text provided.'


if __name__ == '__main__':
    try:
        app.run(host="0.0.0.0",debug=True)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()