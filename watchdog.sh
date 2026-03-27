#!/bin/bash
LOG_DIR="/Users/gtrades/ptnut-bot"
BOT="/Users/gtrades/ptnut-bot/ptnut_bot.py"
PYTHON="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3"
while true; do
    LOG="$LOG_DIR/ptnut_bot_$(TZ=America/Chicago date +%Y-%m-%d).log"
    PID=$(pgrep -f "ptnut_bot.py" | head -1)
    if [ -z "$PID" ]; then
        echo "$(TZ=America/Chicago date '+%H:%M:%S') Bot DEAD — restarting"
        cd "$LOG_DIR"; rm -rf __pycache__
        PYTHONDONTWRITEBYTECODE=1 nohup $PYTHON -Bu $BOT >> "$LOG" 2>&1 &
        sleep 60
    elif [ -f "$LOG" ]; then
        LAST=$(stat -f %m "$LOG" 2>/dev/null || echo 0)
        NOW=$(date +%s)
        DIFF=$((NOW - LAST))
        if [ "$DIFF" -gt 600 ]; then
            echo "$(TZ=America/Chicago date '+%H:%M:%S') FROZEN ${DIFF}s — killing PID $PID"
            kill -9 $PID 2>/dev/null
            sleep 5
            cd "$LOG_DIR"; rm -rf __pycache__
            PYTHONDONTWRITEBYTECODE=1 nohup $PYTHON -Bu $BOT >> "$LOG" 2>&1 &
            sleep 60
        fi
    fi
    sleep 30
done
