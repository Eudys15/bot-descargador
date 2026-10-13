import os
import asyncio
import json
import datetime
import time
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from telethon import TelegramClient, events
from aiohttp import web

# ==========================================
# 1. CONFIGURACIÓN (LEÍDA DESDE VARIABLES DE ENTORNO)
# ==========================================
API_ID = int(os.environ.get('API_ID', 0))
API_HASH = os.environ.get('API_HASH', '')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')

S3_ENDPOINT = os.environ.get('S3_ENDPOINT', '')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', '')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
S3_REGION = os.environ.get('S3_REGION', 'auto')

HORAS_PARA_BORRAR = 23
ARCHIVO_REGISTRO = '/tmp/registro_borrado.json'
DOWNLOAD_DIR = '/tmp/descargas_temp'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ==========================================
# 2. INICIAR CLIENTES (CON CORRECCIÓN PARA BACKBLAZE)
# ==========================================
client = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

b2_config = Config(
    request_checksum_calculation="when_required",
    connect_timeout=30,
    read_timeout=120
)

s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=b2_config
)

# ==========================================
# 3. FUNCIONES DE REGISTRO Y BORRADO
# ==========================================
def cargar_registro():
    if os.path.exists(ARCHIVO_REGISTRO):
        with open(ARCHIVO_REGISTRO, 'r') as f:
            return json.load(f)
    return []

def guardar_registro(registro):
    with open(ARCHIVO_REGISTRO, 'w') as f:
        json.dump(registro, f, indent=4)

def agregar_al_registro(object_name):
    hora_actual = datetime.datetime.now()
    hora_borrado = hora_actual + datetime.timedelta(hours=HORAS_PARA_BORRAR)
    registro = cargar_registro()
    registro.append({
        "object_name": object_name,
        "fecha_borrado": hora_borrado.strftime('%Y-%m-%d %H:%M:%S')
    })
    guardar_registro(registro)

def delete_from_s3(object_name):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=object_name)
        print(f"🗑️ Archivo eliminado de Backblaze: {object_name}")
    except ClientError as e:
        print(f"Error al eliminar de S3: {e}")

async def bucle_limpieza():
    print("🔄 Iniciando bucle de limpieza automática...")
    while True:
        await asyncio.sleep(300)
        registro = cargar_registro()
        ahora = datetime.datetime.now()
        registro_actualizado = []
        for item in registro:
            try:
                fecha_borrado = datetime.datetime.strptime(item['fecha_borrado'], '%Y-%m-%d %H:%M:%S')
                if ahora >= fecha_borrado:
                    delete_from_s3(item['object_name'])
                else:
                    registro_actualizado.append(item)
            except Exception as e:
                print(f"Error procesando registro: {e}")
        if len(registro_actualizado) != len(registro):
            guardar_registro(registro_actualizado)

# ==========================================
# 4. MANEJADORES DEL BOT (PROGRESO 1% CON FRENO DE SEGURIDAD)
# ==========================================
@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond(
        "¡Hola! 👋 Soy tu bot subidor.\n\n"
        "Envíame un video o archivo y lo subiré a la nube.\n"
        "El enlace durará 23 horas."
    )

@client.on(events.NewMessage(incoming=True, func=lambda e: e.media and not e.out))
async def handle_incoming_file(event):
    msg = await event.respond('⏳ **Iniciando proceso...**')
    file_path = None
    
    try:
        # --- FASE DE DESCARGA (PROGRESO 1% CADA 2 SEGUNDOS) ---
        last_download_percent = [0]
        last_download_time = [0]

        async def download_progress(current, total):
            percent = int((current / total) * 100)
            current_time = time.time()
            
            # Actualiza cada 1% pero máximo cada 2 segundos para evitar FloodWait
            if percent == 100 or (percent > last_download_percent[0] and current_time - last_download_time[0] >= 2):
                last_download_percent[0] = percent
                last_download_time[0] = current_time
                barra = '█' * (percent // 10) + '░' * (10 - percent // 10)
                try:
                    await msg.edit(f"⏳ **Descargando de Telegram...**\n`[{barra}] {percent}%`")
                except:
                    pass

        file_path = await event.message.download_media(
            file=DOWNLOAD_DIR,
            progress_callback=download_progress
        )
        
        if event.file and event.file.name:
            object_name = event.file.name
        else:
            object_name = os.path.basename(file_path)

        # --- FASE DE SUBIDA (PROGRESO 1% CADA 2 SEGUNDOS) ---
        total_size = os.path.getsize(file_path)
        last_upload_percent = [0]
        last_upload_time = [0]
        loop = asyncio.get_running_loop()

        def upload_progress(bytes_transferred):
            percent = int((bytes_transferred / total_size) * 100)
            current_time = time.time()
            
            if percent == 100 or (percent > last_upload_percent[0] and current_time - last_upload_time[0] >= 2):
                last_upload_percent[0] = percent
                last_upload_time[0] = current_time
                barra = '█' * (percent // 10) + '░' * (10 - percent // 10)
                text = f"☁️ **Subiendo a Backblaze...**\n`[{barra}] {percent}%`"
                asyncio.run_coroutine_threadsafe(msg.edit(text), loop)

        await msg.edit("☁️ **Subiendo a Backblaze...**\n`[░░░░░░░░░░] 0%`")

        await asyncio.to_thread(
            s3_client.upload_file,
            file_path,
            S3_BUCKET_NAME,
            object_name,
            Callback=upload_progress
        )

        # --- GENERAR ENLACE FINAL ---
        download_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_name},
            ExpiresIn=HORAS_PARA_BORRAR * 3600
        )
        
        agregar_al_registro(object_name)
        
        await msg.edit(
            f"✅ **¡Listo!**\n\n"
            f"📁 **Archivo:** {object_name}\n\n"
            f"🔗 **[Haz clic aquí para descargar]({download_url})**\n\n"
            f"⏳ *El enlace y el archivo se eliminarán en 23 horas.*"
        )

    except Exception as e:
        await msg.edit(f'❌ **Error:**\n`{str(e)}`')
    finally:
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
        except Exception as cleanup_error:
            print(f"No se pudo borrar el archivo temporal: {cleanup_error}")

# ==========================================
# 5. SERVIDOR WEB (OBLIGATORIO PARA RENDER)
# ==========================================
async def handle_web(request):
    return web.Response(text="🤖 Bot funcionando correctamente.")

async def iniciar_servidor_web():
    app = web.Application()
    app.router.add_get('/', handle_web)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Servidor web escuchando en el puerto {port}")

# ==========================================
# 6. EJECUTAR
# ==========================================
if __name__ == '__main__':
    print("=========================================")
    print("🤖 BOT SUBIDOR INICIADO EN RENDER")
    print(f"Los archivos se registrarán y borrarán {HORAS_PARA_BORRAR} horas después.")
    print("=========================================")
    
    client.loop.create_task(iniciar_servidor_web())
    client.loop.create_task(bucle_limpieza())
    
    client.run_until_disconnected()
