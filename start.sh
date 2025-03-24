#!/bin/bash
mkdir -p match_logs
mkdir -p debug_html
gunicorn app:app -b 0.0.0.0:$PORT