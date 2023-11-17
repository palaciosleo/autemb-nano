import pandas as pd
import json
import cx_Oracle
from datetime import datetime


def get_formulas():
    try:
        # Conexion a la base para levantar las formulas
        host = 'racexa03-scan1.gobiernocba.gov.ar'
        port = 1521
        sid = 'moremigd'
        user = 'dc'
        password = 'dc'
        sid = cx_Oracle.makedsn(host, port, service_name=sid)

        with cx_Oracle.connect(user, password, sid, encoding="UTF-8") as connection:
            cursor = connection.cursor()
            cursor.execute(
                'select impuesto, cpto_origen, for_cpto_calculo, cpto_distri, formula_calculo, formula_distri, distribuye, cta_destino from formulas')
            result_set = cursor.fetchall()
            df_formulas = pd.DataFrame(result_set, columns=[col[0] for col in cursor.description])
        return df_formulas
    except Exception as e:
        print(f"Error al traer formulas de DB:{str(e)}")


def get_formulas_local():
    return pd.read_csv('formulas.csv', delimiter='\t')


def calcular_autemb(listPago, df_formulas_calc):
    # df_detalles_pago = pd.DataFrame(listPago['listDetallesPago'])

    # extraigo de la caracteristica el tipo de mov
    df_detalles_pago = pd.json_normalize(listPago['listDetallesPago'], 'caracteristica',
                                         ['idTransaccion', 'tipoMovimiento', 'montoPagado'])
    df_detalles_pago["montoPagado"] = pd.to_numeric(df_detalles_pago["montoPagado"])
    df_detalles_pago = pd.merge(df_detalles_pago, df_formulas_calc, left_on='valor', right_on='for_cpto_origen')

    # Calculo el monto de recargos y descuentos
    indice_general = df_detalles_pago[df_detalles_pago['for_cpto_calculo'] == 'GENERAL'].index[0]
    monto_recargo = df_detalles_pago.loc[indice_general, 'montoPagado']

    # Calcula porcentaje de GENERAL (Redondeo) si se distribuye
    existe_autmun = 'AUTMUN' in df_detalles_pago['for_cpto_origen'].values
    # existe_general = 'GENERAL' in df_calculo['for_cpto_calculo'].values
    distribuye = 'S' in df_detalles_pago['for_prorratea'].values

    if existe_autmun and distribuye:
        df_detalles_pago['montoRecargo'] = monto_recargo * .5
    else:
        df_detalles_pago['montoRecargo'] = monto_recargo

    df = df_detalles_pago  # Se utiliza en 'df_new['montoCalculado'] = pd.eval(formula)'

    # Separo recargos de conceptos validos
    df_recargo = df_detalles_pago[df_detalles_pago['for_cpto_calculo'] == 'GENERAL']
    df_calculo = df_detalles_pago[df_detalles_pago['for_cpto_calculo'] != 'GENERAL']

    # Aplico formula para calculo de monto final
    df_list = []
    for formula in df_calculo['for_formula_calculo'].unique():
        df_new = df_calculo[df_calculo['for_formula_calculo'] == formula].copy()
        df_new['montoCalculado'] = pd.eval(formula)
        df_list.append(df_new)
    df_calculo = pd.concat(df_list, ignore_index=True)

    df_calculo = df_calculo.groupby('for_cpto_calculo')['montoCalculado'].sum().reset_index()
    calculado = df_calculo.rename(columns={'for_cpto_calculo': 'tipoMovimiento', 'montoCalculado': 'montoPagado'})
    calculado['montoPagado'] = calculado['montoPagado'].round(2)

    v_salida = calculado
    v_salida['idTransaccion'] = datetime.now().strftime('%y%m%d%H%M%S%f')[:14]
    v_salida['caracteristica'] = '[{''tipo'': ''CMCCCDC'', ''valor'':'' }]'

    pago_actualizado = listPago
    pago_actualizado['listDetallesPago'] = v_salida.to_dict('records')
    return pago_actualizado


def calcular_inmo(listPago, df_formulas_calc):
    try:
        df_detalles_pago = pd.json_normalize(listPago['listDetallesPago'], 'caracteristica',
                                             ['idTransaccion', 'tipoMovimiento', 'montoPagado'])

        df_detalles_pago = pd.merge(df_detalles_pago, df_formulas_calc, left_on='valor', right_on='for_cpto_origen')

        df_recargo = df_detalles_pago[df_detalles_pago['for_cpto_calculo'] == 'GENERAL']
        df_calculo = df_detalles_pago[df_detalles_pago['for_cpto_calculo'] != 'GENERAL']

        df_calculo2 = df_calculo[['tipoMovimiento', 'for_formula_calculo', 'for_salida_json']]

        df_conceptos = df_calculo.groupby('tipoMovimiento')['montoPagado'].sum().reset_index()

        df_pivot = df_conceptos.pivot(columns='tipoMovimiento', values='montoPagado')
        df_pivot = df_pivot.fillna(0)

        total_row = df_pivot.sum(axis=0).to_frame().T

        df_result = pd.merge(df_calculo2, total_row, how='cross')
        df = df_result
        df_list = []
        for formula in df_result['for_formula_calculo'].unique():
            df_new = df_result[df_result['for_formula_calculo'] == formula].copy()
            df_new['montoCalculado'] = pd.eval(formula)
            df_list.append(df_new)
        df_calculo = pd.concat(df_list, ignore_index=True)

        v_salida = df_calculo
        v_salida['idTransaccion'] = datetime.now().strftime('%y%m%d%H%M%S%f')[:14]
        v_salida['caracteristica'] = v_salida['tipoMovimiento'].apply(
            lambda x: f"[{{'tipo': 'CMCCCDC', 'valor':'{x}'}}]")
        v_salida = v_salida[(v_salida['montoCalculado'] > 0) & (v_salida['for_salida_json'] == 'S')]
        v_salida = v_salida[['tipoMovimiento', 'caracteristica', 'idTransaccion', 'montoCalculado']].round(
            {'montoCalculado': 2})

        pago_actualizado = listPago
        pago_actualizado['listDetallesPago'] = v_salida.to_dict('records')
        return pago_actualizado

    except Exception as e:
        print(str(e))


def main(data):
    estado = 'Procesando'
    # df_formulas = get_formulas()
    df_formulas = get_formulas_local()

    # Separo formulas de calculo y distribucion
    df_formulas_calc = df_formulas.drop(["for_formula_distri", 'for_cta_destino'], axis=1)

    lista_pagos_actualizados = []
    # Recorro los listPagos
    for listPago in data['boleta']['listPagos']:
        # Hacer algo con la cabecera
        if listPago['idTipoObligacion'][:3] in ('AUT', 'EMB'):
            pago_actualizado = calcular_autemb(listPago, df_formulas_calc)
        elif listPago['idTipoObligacion'][:3] in ('INM'):
            df_formulas_calc = df_formulas_calc[df_formulas_calc['for_impuesto'] == 'INM']
            pago_actualizado = calcular_inmo(listPago, df_formulas_calc)
        else:
            pago_actualizado = listPago

        lista_pagos_actualizados.append(pago_actualizado)

    boleta_nueva = data['boleta']
    boleta_nueva['listPagos'] = lista_pagos_actualizados

    print(json.dumps({'boleta': boleta_nueva}))


if __name__ == "__main__":
    with open('ImpuestosTODOS.json', encoding="utf8") as f:
        data = json.load(f)
    main(data)
