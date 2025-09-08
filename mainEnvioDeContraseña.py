# send_mail_merge.py
import os
import csv
from itertools import islice
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From, To, Personalization
import time  # 👈 agregado

# === Configuración ===
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")  # export SENDGRID_API_KEY="..."
TEMPLATE_ID = "d-7c8308c19acd40f6a19204daaa30c459"  # <- reemplaza por tu Template ID
FROM_EMAIL = "administracion@fundacioncolegiosantotomas.com.ar"
FROM_NAME = "Colegio Santo Tomas"
CSV_PATH = "destinatarios.csv"

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
        "usuario": row.get("email", ""),
        "password": row.get("password", "")
    }

    p.dynamic_template_data = dyn
    return p

def load_rows(csv_path):
 with open(csv_path, newline="", encoding="cp1252") as f:
        reader = csv.DictReader(f, delimiter=';')  # <- agregá delimiter=';'
        for row in reader:
            # Validación mínima
            if not row.get("email"):
                continue
            yield row



def send_batch(sg: SendGridAPIClient, rows):
    # Un solo Mail por lote (con múltiples personalizations)
    message = Mail()
    message.from_email = From(FROM_EMAIL, FROM_NAME)
    message.template_id = TEMPLATE_ID

    for row in rows:
        p = build_personalization(row)
        message.add_personalization(p)

    # Importante: NO agregar contenido (content) cuando usas dynamic_template_id
    # El HTML lo provee la plantilla dinámica.
    response = sg.client.mail.send.post(request_body=message.get())
    return response.status_code

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
        time.sleep(36)  # 👈 wsperar 36s por cada correo para no pasar de 100/hora

    print(f"Envío completado: {total} correos.")

if __name__ == "__main__":
    main()
