import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import csv
from itertools import islice
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, From, To, Personalization
import time
from datetime import datetime
import threading

# === Configuración ===
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
TEMPLATE_ID = "d-af1c71228d9c44f6966b9882d09ebc71"
FROM_EMAIL = "administracion@fundacioncolegiosantotomas.com.ar"
FROM_NAME = "Colegio Santo Tomas"
CSV_PATH = ""
LOG_FILE = "emails_enviados.csv"
BATCH_SIZE = 1

# -------------------------------
# Funciones de envío
# -------------------------------
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
                agregar_log(f"✅ Enviado a {row.get('email')} (Familia: {row.get('familia')}, Deuda: {row.get('deuda')})")
            else:
                log_email(row, f"ERROR status={status}")
                agregar_log(f"❌ ERROR {status} con {row.get('email')}")
        return status
    except Exception as e:
        for row in rows:
            log_email(row, f"EXCEPCION: {e}")
            agregar_log(f"❌ EXCEPCIÓN con {row.get('email')} -> {e}")
        raise

def enviar_emails():
    global CSV_PATH
    if not CSV_PATH:
        messagebox.showwarning("Atención", "Primero debes cargar un archivo Excel o CSV.")
        return

    if not SENDGRID_API_KEY:
        messagebox.showerror("Error", "Falta la variable de entorno SENDGRID_API_KEY")
        return

    def tarea():
        try:
            sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
            rows = list(load_rows(CSV_PATH))

            total = 0
            for chunk in chunked(rows, BATCH_SIZE):
                status = send_batch(sg, chunk)
                if status not in (200, 202):
                    raise RuntimeError(f"Fallo al enviar batch, status={status}")
                total += len(chunk)
                agregar_log(f"📩 Progreso: {total} correos enviados...")
                time.sleep(5)  # ajustar según tus límites de SendGrid

            messagebox.showinfo("Éxito", f"Envío completado: {total} correos.")
        except Exception as e:
            messagebox.showerror("Error", f"Ocurrió un problema al enviar correos:\n{e}")

    # ejecutar en hilo para no congelar la ventana
    threading.Thread(target=tarea).start()

# -------------------------------
# Funciones para registro
# -------------------------------
def ver_registro():
    if not os.path.isfile(LOG_FILE):
        messagebox.showinfo("Registro vacío", "Aún no hay correos registrados.")
        return

    registro_win = tk.Toplevel(ventana)
    registro_win.title("Registro de Emails Enviados")
    registro_win.geometry("700x400")

    tree = ttk.Treeview(registro_win, columns=("fecha", "familia", "deuda", "email", "estado"), show="headings")
    tree.heading("fecha", text="Fecha y Hora")
    tree.heading("familia", text="Familia")
    tree.heading("deuda", text="Monto Deuda")
    tree.heading("email", text="Destinatario")
    tree.heading("estado", text="Estado")

    tree.pack(fill=tk.BOTH, expand=True)

    with open(LOG_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tree.insert("", tk.END, values=(row["fecha_hora"], row["familia"], row["deuda"], row["email"], row["estado"]))

# -------------------------------
# Ventana gráfica
# -------------------------------
def cargar_excel():
    global CSV_PATH
    archivo = filedialog.askopenfilename(
        title="Selecciona un archivo Excel/CSV",
        filetypes=[("Archivos Excel/CSV", "*.xlsx *.xls *.csv")]
    )
    if archivo:
        try:
            if archivo.endswith(".csv"):
                df = pd.read_csv(archivo, delimiter=";")
            else:
                df = pd.read_excel(archivo)

            CSV_PATH = archivo
            messagebox.showinfo("Éxito", f"Archivo cargado: {len(df)} filas.")
            agregar_log(f"📂 Archivo cargado: {archivo} ({len(df)} filas)")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo leer el archivo:\n{e}")

def agregar_log(mensaje):
    log_text.insert(tk.END, mensaje + "\n")
    log_text.see(tk.END)

# Crear ventana
ventana = tk.Tk()
ventana.title("Enviador de Emails")
ventana.geometry("600x400")

btn_cargar = tk.Button(ventana, text="Cargar Excel/CSV", command=cargar_excel)
btn_cargar.pack(pady=10)

btn_enviar = tk.Button(ventana, text="Enviar Emails", command=enviar_emails)
btn_enviar.pack(pady=10)

btn_registro = tk.Button(ventana, text="Ver Registro", command=ver_registro)
btn_registro.pack(pady=10)

# Caja de texto para mostrar el log
log_text = tk.Text(ventana, height=15, width=70)
log_text.pack(padx=10, pady=10)

ventana.mainloop()
