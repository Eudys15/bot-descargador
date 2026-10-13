import os
import asyncio
import json
import datetime
import boto3
from botocore.exceptions import ClientError
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
ARCHIVO_REGISTRO = 'registro_borrado.json'
DOWNLOAD_DIR = 'descargas_temp'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ==========================================
# 2. INICIAR CLIENTES
# ==========================================
client = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION
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
    print(f"📝 Registrado: {object_name} se borrará el {hora_borrado.strftime('%Y-%m-%d %H:%M:%S')}")

def upload_to_s3(file_path, object_name):
    try:
        s3_client.upload_file(file_path, S3_BUCKET_NAME, object_name)
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_name},
            ExpiresIn=HORAS_PARA_BORRAR * 3600
        )
        return url
    except ClientError as e:
        print(f"Error al subir a S3: {e}")
        return None

def delete_from_s3(object_name):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=object_name)
        print(f"🗑️ Archivo eliminado de Backblaze: {object_name}")
    except ClientError as e:
        print(f"Error al eliminar de S3: {e}")

async def bucle_limpieza():
    print("🔄 Iniciando bucle de limpieza automática...")
    while True:
        await asyncio.sleep(300) # Revisar cada 5 minutos
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
# 4. MANEJADORES DEL BOT
# ==========================================
@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond(
        "¡Hola! 👋 Soy tu bot subidor.\n\n"
        "Envíame un video o archivo (como documento) y lo subiré a Backblaze.\n"
        "El enlace y el archivo durarán 23 horas."
    )

@client.on(events.NewMessage)
async def handle_incoming_file(event):
    if not event.message.media:
        if event.message.text and not event.message.text.startswith('/'):
            await event.respond("⚠️ Por favor, envíame un archivo o video para subirlo a la nube.")
        return

    msg = await event.respond('⏳ **Descargando archivo desde Telegram...**')
    
    file_path = None
    try:
        file_path = await event.message.download_media(file=DOWNLOAD_DIR)
        
        if event.file and event.file.name:
            object_name = event.file.name
        else:
            object_name = os.path.basename(file_path)

        await msg.edit('☁️ **Subiendo a Backblaze... (esto puede tardar según el tamaño)**')
        
        download_url = upload_to_s3(file_path, object_name)

        if download_url:
            agregar_al_registro(object_name)
            await msg.edit(
                f"✅ **¡Listo!**\n\n"
                f"📁 **Archivo:** {object_name}\n\n"
                f"🔗 **[Haz clic aquí para descargar]({download_url})**\n\n"
                f"⏳ *El enlace y el archivo se eliminarán en 23 horas.*"
            )
        else:
            await msg.edit('❌ **Error al subir a Backblaze. Revisa las claves o los permisos.**')

    except Exception as e:
        await msg.edit(f'❌ **Error:**\n`{str(e)}`')
    finally:
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                print(f"Archivo temporal borrado del teléfono: {file_path}")
        except Exception as cleanup_error:
            print(f"No se pudo borrar el archivo temporal: {cleanup_error}")

# ==========================================
# 5. SERVIDOR WEB (OBLIGATORIO PARA RENDER)
# ==========================================
async def handle_web(request):
    """Responde a las visitas de Render para que no apague el bot."""
    return web.Response(text="🤖 Bot de Telegram funcionando correctamente.")

async def iniciar_servidor_web():
    app = web.Application()
    app.router.add_get('/', handle_web)
    runner = web.AppRunner(app)
    await runner.setup()
    # Render asigna un puerto dinámico en la variable de entorno PORT
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Servidor web escuchando en el puerto {port} (para mantener a Render despierto)")

# ==========================================
# 6. EJECUTAR
# ==========================================
if __name__ == '__main__':
    print("=========================================")
    print("🤖 BOT SUBIDOR INICIADO EN RENDER")
    print(f"Los archivos se registrarán y borrarán {HORAS_PARA_BORRAR} horas después.")
    print("=========================================")
    
    # Iniciar tareas asíncronas en segundo plano
    client.loop.create_task(iniciar_servidor_web())
    client.loop.create_task(bucle_limpieza())
    
    # Mantener el bot de Telegram escuchando
    client.run_until_disconnected()
