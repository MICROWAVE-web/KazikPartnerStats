# KazikPartnerStats

Простой Telegram-бот и Flask-сервер для приема постбеков и отчётности по реферальной системе.

## Возможности
- Редактируемое меню с inline-кнопками
- Команда `/generate` — генерация ссылок по шаблону
- Прием постбеков:
  - `/<telegram_user_id>/registration?btag=...&campaign_id=...`
  - `/<telegram_user_id>/firstdep?btag=...&campaign_id=...`
- Статистика с группировкой по компаниям (campaign_id) и btag: регистрации, первые депозиты, сумма вознаграждений
- Снимок вознаграждения фиксируется в момент первого депозита
- Отчеты: совокупный, за месяц, за неделю, за день
- Пользователь может задать/изменить вознаграждение

## Установка
1. Создайте `.env` рядом с `config.py`:
```
BOT_TOKEN=xxxx:yyyy
PREFIX=http://localhost:5000
FLASK_HOST=0.0.0.0
FLASK_PORT=8000
DEFAULT_REWARD_PER_DEP=0
CAMPAIGN_NAMES=campaign_id1:Company Name 1,campaign_id2:Company Name 2
```
2. Установите зависимости:
```
pip install -r requirements.txt
```
3. Запустите:
```
python run.py
```

## Примечания
- База — SQLite файл `data.sqlite3`.
- Aiogram 3 (long polling). Flask запускается в отдельном потоке.
- Изменение вознаграждения влияет только на будущие первые депозиты; суммы по уже пришедшим не меняются.

