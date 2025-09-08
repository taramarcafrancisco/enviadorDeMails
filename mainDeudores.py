# send_mail_merge.py
import os
import csv
from itertools import islice
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From, To, Personalization
import time
from datetime import datetime

# === Configuración ===
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")  # export SENDGRID_API_KEY="..."
TEMPLATE_ID = "d-af1c71228d9c44f6966b9882d09ebc71"  # <- reemplaza por tu Template ID
FROM_EMAIL = "administracion@fundacioncolegiosantotomas.com.ar"
FROM_NAME = "Colegio Santo Tomas"
CSV_PATH = "deudores.csv"
LOG_FILE = "emails_enviados.csv"

# Tamaño de lote: hasta 1000 personalizations por request (recomendado 500-1000)
BATCH_SIZE = 1

def chunked(iterable, size): 
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

def build_personalization(row):
    p = Personalization()
    p.add_to(To(email=row["email"]))

    dyn = {
       
        "familia": row.get("familia", ""),
        "deuda": row.get("deuda", ""),
        "email": row.get("email", "")
    }

    p.dynamic_template_data = dyn
    return p

def load_rows(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            if not row.get("email"):
                continue
            yield row

# --- NUEVO: función para registrar envíos ---
def log_email(row, status="ENVIADO"):
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["fecha_hora", "familia", "deuda", "email", "estado"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        
            row.get("familia", ""),
            row.get("deuda", ""),
            row.get("email", ""),
            status
        ])

def send_batch(sg: SendGridAPIClient, rows):
    message = Mail()
    message.from_email = From(FROM_EMAIL, FROM_NAME)
    message.template_id = TEMPLATE_ID

    for row in rows:
        p = build_personalization(row)
        message.add_personalization(p)

    try:
        response = sg.client.mail.send.post(request_body=message.get())
        status = response.status_code
        for row in rows:
            if status in (200, 202):
                log_email(row, "ENVIADO")
            else:
                log_email(row, f"ERROR status={status}")
        return status
    except Exception as e:
        for row in rows:
            log_email(row, f"EXCEPCION: {e}")
        raise

def main():
    if not SENDGRID_API_KEY:
        raise RuntimeError("Falta la variable de entorno SENDGRID_API_KEY")
    if not TEMPLATE_ID.startswith("d-"):
        raise RuntimeError("TEMPLATE_ID no parece una Dynamic Template (debe empezar con 'd-')")

    sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
    rows = list(load_rows(CSV_PATH))

    total = 0
    for chunk in chunked(rows, BATCH_SIZE):
        status = send_batch(sg, chunk)
        if status not in (200, 202):
            raise RuntimeError(f"Fallo al enviar batch, status={status}")
        total += len(chunk)
        print(f"Enviados {total} correos...")
        time.sleep(36)  # evitar superar 100/hora

    print(f"Envío completado: {total} correos.")

if __name__ == "__main__":
    main()
