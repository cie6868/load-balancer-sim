FROM python:3.12-slim

ENV lb_type="push" worker_id=6800 power=100 lb_hostname="127.0.0.1" lb_port="6799"

WORKDIR /opt/lb
COPY . .

RUN pip install -r ./requirements.txt

RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]

