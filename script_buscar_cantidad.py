import time
import pandas as pd

from funciones import buscar_debitos_segun_cantidad

import logging

logging.basicConfig(level=logging.INFO)


def main():
    print("Emparejamiento de créditos y débitos según cantidad")
    time_start = time.time()
    emparejamiento_cantidad = buscar_debitos_segun_cantidad(cantidad=1000.82, tolerancia=0.01)
    if not emparejamiento_cantidad:
        print("No se encontraron debitos que no tengan creditos")
    print(emparejamiento_cantidad)
    time_end = time.time()
    rows = []
    group_id = 1
    balance = 0
    for debito in emparejamiento_cantidad:
        balance -= debito[1]
        rows.append((group_id, debito[0], debito[1], 0, balance))
    rows.append((group_id, 'balance', 0, 0, balance))
    df_emparejamientos = pd.DataFrame(rows, columns=['group_id', 'id_transaccion', 'debito', 'credito', 'balance'])
    df_emparejamientos.to_excel('emparejamientos_cantidad.xlsx', index=False)
    print(f"Tiempo de ejecución: {time_end - time_start} segundos")


if __name__ == "__main__":
    main()
