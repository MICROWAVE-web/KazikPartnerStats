from flask import Flask, request, jsonify

from config import FLASK_HOST, FLASK_PORT
from db import init_db, insert_event

app = Flask(__name__)


@app.route('/<int:telegram_user_id>/registration', methods=['GET', 'POST'])
def registration(telegram_user_id: int):
    player_id = '-'
    btag = request.args.get('btag')
    campaign_id = request.args.get('campaign_id')
    insert_event(telegram_user_id, 'registration', player_id, btag, campaign_id)
    return jsonify({"status": "ok"})


@app.route('/<int:telegram_user_id>/firstdep', methods=['GET', 'POST'])
def first_dep(telegram_user_id: int):
    player_id = '-'
    btag = request.args.get('btag')
    campaign_id = request.args.get('campaign_id')
    insert_event(telegram_user_id, 'first_dep', player_id, btag, campaign_id)
    return jsonify({"status": "ok"})


def run_flask():
    init_db()
    app.run(host=FLASK_HOST, port=FLASK_PORT, threaded=True)
