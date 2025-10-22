import asyncio
import csv
import time
from bleak import BleakScanner, BleakClient

async def device_handler(dispositivo, log_filename, rssi):
    """
    Maneja la conexión y recepción de notificaciones de un dispositivo BLE,
    guardando los datos en un fichero CSV propio.
    """
    CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"
    notification_queue = asyncio.Queue()
    packet_count = 0

    def notification_handler(sender: int, data: bytearray):
        """
        Callback para notificaciones BLE. Se asume que el buffer tiene 244 bytes:
          - Los primeros 240 bytes: datos en crudo (I y Q intercalados).
          - Últimos 4 bytes: número de secuencia (little endian).
        """
        if len(data) < 244:
            return
        raw_data = data[:240]
        # Se extrae la secuencia (últimos 2 bytes, como en el código original)
        sequence = int.from_bytes(data[-2:], byteorder="little")
        sequence = int((sequence + 1) / 30)
        notification_queue.put_nowait((sequence, raw_data))

    async def process_notifications():
        """
        Consume las notificaciones encoladas y las escribe en el CSV de forma batch.
        """
        nonlocal packet_count
        batch = []
        BATCH_SIZE = 1000  # Escribe cada 1000 notificaciones
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
                if batch:
                    writer.writerows(batch)
                    csvfile.flush()
                raise

    async with BleakClient(dispositivo.address) as client:
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Conectado a {dispositivo.name} ({dispositivo.address}). Datos se guardarán en '{log_filename}'.")
        consumer_task = asyncio.create_task(process_notifications())
        try:
            # Bucle principal: se verifica cada segundo si se han recibido 100,000 paquetes
            while True:
                await asyncio.sleep(1)
                if packet_count >= 100000:
                    print(f"Se han recibido 100,000 paquetes de {dispositivo.name}. Deteniendo notificaciones...")
                    await client.stop_notify(CHARACTERISTIC_UUID)
                    break
        except KeyboardInterrupt:
            print(f"Desconectando {dispositivo.name} por interrupción de teclado...")
        finally:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            print(f"Total de paquetes recibidos para {dispositivo.name}: {packet_count}")

async def run():
    # Se pide la distancia para incorporar en el nombre del fichero
    distancia = input("Ingresa la distancia a la que se ha medido: ").strip()

    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()

    if len(dispositivos) < 2:
        print("No se encontraron al menos dos dispositivos.")
        return

    # Se muestran los dispositivos encontrados
    for idx, dispositivo in enumerate(dispositivos):
        print(f"{idx}: {dispositivo.name} - {dispositivo.address} - RSSI: {dispositivo.rssi} dBm")

    indices_input = input("Ingresa los índices de los dos dispositivos a conectar, separados por coma: ")
    indices_str = indices_input.split(",")
    if len(indices_str) != 2:
        print("Se deben ingresar exactamente dos índices.")
        return

    try:
        index1 = int(indices_str[0].strip())
        index2 = int(indices_str[1].strip())
    except ValueError:
        print("Índices inválidos.")
        return

    if index1 < 0 or index1 >= len(dispositivos) or index2 < 0 or index2 >= len(dispositivos):
        print("Algún índice está fuera de rango.")
        return

    if index1 == index2:
        print("Los dos índices deben ser distintos.")
        return

    dispositivo1 = dispositivos[index1]
    dispositivo2 = dispositivos[index2]

    # Se crean nombres de archivo únicos para cada dispositivo (se limpia la dirección para formar el nombre)
    safe_address1 = dispositivo1.address.replace(":", "")
    safe_address2 = dispositivo2.address.replace(":", "")
    log_filename1 = f"hp_cw_dist_{distancia}m_{safe_address1}.csv"
    log_filename2 = f"hp_cw_dist_{distancia}m_{safe_address2}.csv"

    print("Conectando a dos dispositivos BLE...")

    task1 = asyncio.create_task(device_handler(dispositivo1, log_filename1, dispositivo1.rssi))
    task2 = asyncio.create_task(device_handler(dispositivo2, log_filename2, dispositivo2.rssi))

    await asyncio.gather(task1, task2)

if __name__ == "__main__":
    asyncio.run(run())

