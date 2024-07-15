import time
from funciones import emparejar_creditos_debitos, emparejar_creditos_debitos_backtracking
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO)


def main():
    print("Emparejamiento de créditos y débitos")
    start_time = time.time()
    emparejamientos = emparejar_creditos_debitos()
    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time} segundos")
    for emparejamiento in emparejamientos:
        print(emparejamiento)
    rows = []
    for group_id, (credito, debitos) in enumerate(emparejamientos, start=1):
        balance = 0
        balance += credito[1]
        rows.append((group_id, credito[0], 0, credito[1], balance))
        for debito in debitos:
            balance -= debito[1]
            rows.append((group_id, debito[0], debito[1], 0, balance))
        rows.append((group_id, 'balance', 0, 0, balance))
    df_emparejamientos = pd.DataFrame(rows, columns=['group_id', 'id_transaccion', 'debito', 'credito', 'balance'])
    df_emparejamientos.to_excel('emparejamientos.xlsx', index=False)
    start_time = time.time()
    emparejamiento_backtracking = emparejar_creditos_debitos_backtracking()
    for emparejamiento in emparejamiento_backtracking:
        print(emparejamiento)
    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time} segundos")
    rows = []
    for group_id, (credito, debitos) in enumerate(emparejamiento_backtracking, start=1):
        balance = 0
        balance += credito[1]
        rows.append((group_id, credito[0], 0, credito[1], balance))
        for debito in debitos:
            balance -= debito[1]
            rows.append((group_id, debito[0], debito[1], 0, balance))
        rows.append((group_id, 'balance', 0, 0, balance))
    df_emparejamientos = pd.DataFrame(rows, columns=['group_id', 'id_transaccion', 'debito', 'credito', 'balance'])
    df_emparejamientos.to_excel('emparejamientos_backtracking.xlsx', index=False)


if __name__ == "__main__":
    main()
