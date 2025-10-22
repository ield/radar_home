import asyncio
import csv
import os
import time
from bleak import BleakScanner, BleakClient

# Archivo de log y contador global
log_filename = None
packet_count = 0

# Cola asíncrona para notificaciones
notification_queue = asyncio.Queue()
device_rssi = {}  # Almacena el RSSI obtenido en el escaneo

def notification_handler(sender: int, data: bytearray):
    """
    Callback de BLE.
    Se asume que el buffer tiene 244 bytes:
      - Los primeros 240 bytes: datos en crudo (I y Q intercalados).
      - Últimos 4 bytes: número de secuencia (little endian).
    """
    if len(data) < 244:
        return

    raw_data = data[:240]
    # Se extrae la secuencia completa de los últimos 4 bytes en little-endian
    sequence = int.from_bytes(data[-2:], byteorder="little")
    sequence = int((sequence + 1) / 30)
    # Encolamos la tupla (sequence, raw_data)
    notification_queue.put_nowait((sequence, raw_data))

async def process_notifications(selected_device):
    """
    Consume las notificaciones encoladas y las escribe en el CSV de forma batch.
    Se utiliza el RSSI obtenido durante el escaneo.
    """
    global packet_count
    rssi = device_rssi.get(selected_device.address, None)
    batch = []
    BATCH_SIZE = 1000  # Escribiremos cada 1000 notificaciones

    # Se abre el archivo una única vez (sobrescribiendo) y se escribe el encabezado
    with open(log_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "Sequence", "RSSI", "RawData"])
        try:
            while True:
                sequence, raw_data = await notification_queue.get()
                packet_count += 1
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                raw_str = ';'.join(str(b) for b in raw_data)
                batch.append([timestamp, sequence, rssi, raw_str])
                if len(batch) >= BATCH_SIZE:
                    writer.writerows(batch)
                    csvfile.flush()
                    batch.clear()
        except asyncio.CancelledError:
            # Al cancelar, se escribe lo que quede en el lote
            if batch:
                writer.writerows(batch)
                csvfile.flush()
            raise

async def run():
    global log_filename, packet_count
    CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"
    NAME_REF = "Zenith_CW_1"
    
    log_filename = f"{NAME_REF}.csv"
    
    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()
    
    if not dispositivos:
        print("No se encontraron dispositivos.")
        return

    # Escoge el dispositivo indicado
    dispositivo_seleccionado = 0
    for idx, dispositivo in enumerate(dispositivos):
        if dispositivo.name == NAME_REF:
            dispositivo_seleccionado = dispositivo

    if dispositivo_seleccionado == 0:
        print("Dispositivo no encontrado")
        return

    print("Conectando a:", dispositivo_seleccionado.name, dispositivo_seleccionado.address)

    max_packet = 1000

    async with BleakClient(dispositivo_seleccionado.address) as client:
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Conectado.")
        
        # Iniciamos la tarea para procesar notificaciones
        consumer_task = asyncio.create_task(process_notifications(dispositivo_seleccionado))
        
        try:
            # Bucle principal: se verifica el numero de paquetes recibidos cada segundo
            while True:
                await asyncio.sleep(1)
                if packet_count >= max_packet:
                    print(f"Se han recibido '{max_packet}' paquetes. Deteniendo notificaciones...")
                    await client.stop_notify(CHARACTERISTIC_UUID)
                    break
        except KeyboardInterrupt:
            print("Desconectando por interrupción de teclado...")
        finally:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            print(f"Total de paquetes recibidos: {packet_count}")

if __name__ == "__main__":
    asyncio.run(run())
