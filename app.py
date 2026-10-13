import os
import asyncio
import json
import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from telethon import TelegramClient, events
from aiohttp import web

# ==========================================
# 1. CONFIGURACIÓN (LEÍDA DESDE VARIABLES DE ENTORNO)
# ==========================================
# Limpiamos los valores con .strip() para eliminar espacios, saltos de línea 
# y comillas invisibles que se cuelan al copiar/pegar en Render.

API_ID = int(os.environ.get('API_ID', '0').strip())
API_HASH = os.environ.get('API_HASH', '').strip().strip('"').strip("'")
BOT_TOKEN = os.environ.get('BOT_TOKEN', '').strip().strip('"').strip("'")

S3_ENDPOINT = os.environ.get('S3_ENDPOINT', '').strip().strip('"').strip("'")
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY', '').strip().strip('"').strip("'")
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY', '').strip().strip('"').strip("'")
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '').strip().strip('"').strip("'")
S3_REGION = os.environ.get('S3_REGION', 'auto').strip().strip('"').strip("'")

# --- DIAGNÓSTICO EN CONSOLA ---
print(f"🔑 Longitud de S3_ACCESS_KEY: {len(S3_ACCESS_KEY)} caracteres")
print(f"🔑 Primeros 4 caracteres: {S3_ACCESS_KEY[:4]}")
print(f"🔑 Últimos 4 caracteres: {S3_ACCESS_KEY[-4:]}")
# ------------------------------

HORAS_PARA_BORRAR = 23
ARCHIVO_REGISTRO = '/tmp/registro_borrado.json'
PART_SIZE = 50 * 1024 * 1024  # 50 MB por parte para subida multiparte

# ==========================================
# 2. INICIAR CLIENTES (CON CORRECCIÓN PARA BACKBLAZE)
# ==========================================
client = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Configuración especial para evitar el error de conexión con Backblaze B2
b2_config = Config(
    request_checksum_calculation="when_required",
    response_checksum_validation="when_required",
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    connect_timeout=60,
    read_timeout=300,
    max_pool_connections=10,
    tcp_keepalive=True
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
# 4. FUNCIÓN DE STREAMING MULTIPARTE (TELEGRAM -> S3)
# ==========================================
async def stream_telegram_to_s3_multipart(message, object_name, status_msg):
    total_size = message.file.size if message.file else 0
    
    # Iniciar subida multiparte
    mpu = await asyncio.to_thread(
        s3_client.create_multipart_upload,
        Bucket=S3_BUCKET_NAME,
        Key=object_name
    )
    upload_id = mpu['UploadId']
    parts = []
    part_number = 1
    buffer = bytearray()
    bytes_subidos = 0

    try:
        # Iterar sobre los fragmentos de Telegram
        async for chunk in client.iter_download(message.media, chunk_size=1024*1024): # 1 MB chunks de Telegram
            buffer.extend(chunk)
            
            # Cuando el búfer alcanza los 50 MB, subimos una parte
            if len(buffer) >= PART_SIZE:
                part = await asyncio.to_thread(
                    s3_client.upload_part,
                    Bucket=S3_BUCKET_NAME,
                    Key=object_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=bytes(buffer)
                )
                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                part_number += 1
                bytes_subidos += len(buffer)
                buffer = bytearray()
                
                # Actualizar progreso
                if total_size > 0:
                    percent = int((bytes_subidos / total_size) * 100)
                    barra = '█' * (percent // 10) + '░' * (10 - percent // 10)
                    try:
                        await status_msg.edit(f"☁️ **Subiendo a Backblaze...**\n`[{barra}] {percent}%`")
                    except:
                        pass

        # Subir la última parte (si queda algo en el búfer)
        if len(buffer) > 0:
            part = await asyncio.to_thread(
                s3_client.upload_part,
                Bucket=S3_BUCKET_NAME,
                Key=object_name,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=bytes(buffer)
            )
            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
            bytes_subidos += len(buffer)

        # Completar la subida multiparte
        await asyncio.to_thread(
            s3_client.complete_multipart_upload,
            Bucket=S3_BUCKET_NAME,
            Key=object_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        return True

    except Exception as e:
        # Si hay error, abortar la subida multiparte para no dejar basura en S3
        try:
            await asyncio.to_thread(s3_client.abort_multipart_upload, Bucket=S3_BUCKET_NAME, Key=object_name, UploadId=upload_id)
        except:
            pass
        raise e

# ==========================================
# 5. MANEJADORES DEL BOT
# ==========================================
@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond(
        "¡Hola! 👋 Soy tu bot subidor.\n\n"
        "Envíame un video o archivo (como documento) y lo subiré a la nube.\n"
        "El enlace durará 23 horas."
    )

@client.on(events.NewMessage(incoming=True, func=lambda e: e.media and not e.out))
async def handle_incoming_file(event):
    msg = await event.respond('⏳ **Iniciando proceso...**')
    
    try:
        if event.file and event.file.name:
            object_name = event.file.name
        else:
            object_name = f"archivo_{int(datetime.datetime.now().timestamp())}.mp4"

        await msg.edit("☁️ **Subiendo a Backblaze...**\n`[░░░░░░░░░░] 0%`")
        
        # Iniciar streaming multiparte
        await stream_telegram_to_s3_multipart(event.message, object_name, msg)

        # Generar enlace final
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

# ==========================================
# 6. SERVIDOR WEB (OBLIGATORIO PARA RENDER)
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
# 7. EJECUTAR
# ==========================================
if __name__ == '__main__':
    print("=========================================")
    print("🤖 BOT SUBIDOR INICIADO EN RENDER (STREAMING MULTIPARTE)")
    print(f"Los archivos se registrarán y borrarán {HORAS_PARA_BORRAR} horas después.")
    print("=========================================")
    
    client.loop.create_task(iniciar_servidor_web())
    client.loop.create_task(bucle_limpieza())
    
    client.run_until_disconnected()
