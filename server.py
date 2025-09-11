from flask import Flask, request, jsonify

from config import FLASK_HOST, FLASK_PORT
from db import init_db, insert_event


app = Flask(__name__)


@app.route('/<int:telegram_user_id>/registration', methods=['GET', 'POST'])
def registration(telegram_user_id: int):
    played_id = request.args.get('player_id') or request.form.get('player_id')
    btag = request.args.get('btag') or request.form.get('btag')
    insert_event(telegram_user_id, 'registration', played_id, btag)
    return jsonify({"status": "ok"})


@app.route('/<int:telegram_user_id>/firstdep', methods=['GET', 'POST'])
def first_dep(telegram_user_id: int):
    played_id = request.args.get('player_id') or request.form.get('player_id')
    btag = request.args.get('btag') or request.form.get('btag')
    insert_event(telegram_user_id, 'first_dep', played_id, btag)
    return jsonify({"status": "ok"})


def run_flask():
    init_db()
    app.run(host=FLASK_HOST, port=FLASK_PORT, threaded=True)


