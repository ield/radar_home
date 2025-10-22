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
      - Últimos 4 bytes: 2 bytes para el número de secuencia y 2 para el número de bin (little endian).
    Se transforma la secuencia: (num + 1)/20.
    """
    if len(data) < 244:
        return

    raw_data = data[:240]
    tail_bytes = data[-4:]
    sequence = int.from_bytes(tail_bytes[0:2], byteorder="little")
    bin_val = int.from_bytes(tail_bytes[2:4], byteorder="little")
    sequence = int((sequence + 1) / 20)
    # Encolamos la tupla (sequence, bin, raw_data)
    notification_queue.put_nowait((sequence, bin_val, raw_data))

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
        writer.writerow(["Timestamp", "Sequence", "Bin", "RSSI", "RawData"])
        try:
            while True:
                sequence, bin_val, raw_data = await notification_queue.get()
                packet_count += 1
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                raw_str = ';'.join(str(b) for b in raw_data)
                batch.append([timestamp, sequence, bin_val, rssi, raw_str])
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
    
    # Se pregunta la distancia y se define el nombre del archivo de log
    distancia = input("Ingresa la distancia a la que se ha medido: ").strip()
    log_filename = f"hp_binSelect_dist_{distancia}m.csv"
    
    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()
    
    if not dispositivos:
        print("No se encontraron dispositivos.")
        return

    # Se guarda el RSSI obtenido en el escaneo
    for dispositivo in dispositivos:
        device_rssi[dispositivo.address] = dispositivo.rssi

    # Se muestran los dispositivos encontrados
    for idx, dispositivo in enumerate(dispositivos):
        print(f"{idx}: {dispositivo.name} - {dispositivo.address} - RSSI: {dispositivo.rssi} dBm")

    try:
        indice = int(input("Ingresa el índice del dispositivo a conectar: "))
    except ValueError:
        print("Índice inválido.")
        return

    if indice < 0 or indice >= len(dispositivos):
        print("Índice fuera de rango.")
        return

    dispositivo_seleccionado = dispositivos[indice]
    print("Conectando a:", dispositivo_seleccionado.name, dispositivo_seleccionado.address)

    async with BleakClient(dispositivo_seleccionado.address) as client:
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Conectado. Los datos se guardarán en '{log_filename}'.")
        
        # Iniciamos la tarea para procesar notificaciones
        consumer_task = asyncio.create_task(process_notifications(dispositivo_seleccionado))
        
        try:
            # Bucle principal: se verifica cada segundo si se han recibido 100,000 paquetes
            while True:
                await asyncio.sleep(1)
                if packet_count >= 100000:
                    print("Se han recibido 100,000 paquetes. Deteniendo notificaciones...")
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
