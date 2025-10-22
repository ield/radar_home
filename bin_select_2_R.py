import asyncio
import csv
import time
from bleak import BleakScanner, BleakClient

async def run_device(device, distancia, MAX_PACKETS=100000, BATCH_SIZE=1000):
    """
    Conecta a un dispositivo BLE, inicia la notificación, procesa y guarda
    los datos en un archivo CSV específico para ese dispositivo.
    """
    # Se forma un nombre de archivo único usando la dirección (sin ':')
    device_id = device.address.replace(":", "")
    log_filename = f"hp_binSelect_dist_{distancia}m_{device_id}.csv"
    
    print(f"Conectando a: {device.name} - {device.address}. Log: '{log_filename}'")
    
    notification_queue = asyncio.Queue()
    packet_count = 0
    rssi = device.rssi  # RSSI obtenido durante el escaneo

    # Callback para notificaciones BLE
    def notification_handler(sender: int, data: bytearray):
        nonlocal packet_count
        if len(data) < 244:
            return
        raw_data = data[:240]
        tail_bytes = data[-4:]
        sequence = int.from_bytes(tail_bytes[0:2], byteorder="little")
        bin_val = int.from_bytes(tail_bytes[2:4], byteorder="little")
        sequence = int((sequence + 1) / 20)
        notification_queue.put_nowait((sequence, bin_val, raw_data))
    
    async def process_notifications():
        nonlocal packet_count
        batch = []
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
                if batch:
                    writer.writerows(batch)
                    csvfile.flush()
                raise

    CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"
    
    async with BleakClient(device.address) as client:
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Conectado a {device.name}. Comenzando a recibir notificaciones...")
        
        consumer_task = asyncio.create_task(process_notifications())
        
        try:
            while True:
                await asyncio.sleep(1)
                if packet_count >= MAX_PACKETS:
                    print(f"{device.name} ha recibido {MAX_PACKETS} paquetes. Deteniendo notificaciones...")
                    await client.stop_notify(CHARACTERISTIC_UUID)
                    break
        except KeyboardInterrupt:
            print(f"Desconectando {device.name} por interrupción de teclado...")
        finally:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            print(f"Total de paquetes recibidos de {device.name}: {packet_count}")

async def run():
    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()
    
    if not dispositivos:
        print("No se encontraron dispositivos.")
        return

    # Se muestran todos los dispositivos encontrados
    for idx, dispositivo in enumerate(dispositivos):
        print(f"{idx}: {dispositivo.name} - {dispositivo.address} - RSSI: {dispositivo.rssi} dBm")
    
    try:
        indices = input("Ingresa los índices de los dos dispositivos a conectar (separados por coma): ")
        idx1, idx2 = map(int, indices.split(','))
    except ValueError:
        print("Índices inválidos.")
        return
    
    if idx1 < 0 or idx1 >= len(dispositivos) or idx2 < 0 or idx2 >= len(dispositivos):
        print("Algún índice está fuera de rango.")
        return
    
    if idx1 == idx2:
        print("Debes seleccionar dos dispositivos distintos.")
        return

    dispositivo1 = dispositivos[idx1]
    dispositivo2 = dispositivos[idx2]
    
    distancia = input("Ingresa la distancia a la que se ha medido: ").strip()
    
    # Se ejecutan ambas conexiones en paralelo
    await asyncio.gather(
        run_device(dispositivo1, distancia),
        run_device(dispositivo2, distancia)
    )

if __name__ == "__main__":
    asyncio.run(run())
