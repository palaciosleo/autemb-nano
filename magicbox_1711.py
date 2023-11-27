import pandas as pd
import json
import os
#import cx_Oracle
from datetime import datetime

def main(data):
    #estado = 'Procesando'
    #Conexion a la base para levantar las formulas
    host='racexa03-scan1.gobiernocba.gov.ar'
    port=1521
    sid='moremigd'
    user='dc'
    password='dc'
    #sid = cx_Oracle.makedsn(host, port, service_name=sid)

    """connection = cx_Oracle.connect(user, password, sid, encoding="UTF-8")
    cursor = connection.cursor()
    cursor.execute('SELECT FOR_IMPUESTO, FOR_CPTO_ORIGEN, FOR_CPTO_CALCULO, FOR_FORMULA_CALCULO, FOR_PRORRATEA, FOR_VIGENCIA_DESDE, FOR_VIGENCIA_HASTA FROM FORMULAS WHERE FOR_FECHA_BAJA IS NULL')
    result_set = cursor.fetchall()
    df_formulas = pd.DataFrame(result_set, columns=[col[0] for col in cursor.description])
    # Close the cursor and the database connection
    cursor.close()
    connection.close()"""
    df_formulas = pd.read_csv('formulas_1711.csv', delimiter=';')
    df_formulas_calc = df_formulas
    # Separo formulas de calculo y distribucion
    df_formulas_calc = df_formulas_calc.drop_duplicates(subset=['FOR_IMPUESTO', 'FOR_CPTO_ORIGEN', 'FOR_CPTO_CALCULO', 'FOR_FORMULA_CALCULO', 'FOR_PRORRATEA'])

    # Abro archivo JSON
    #with open('AutomotorVarios.json', encoding="utf8") as f:
    #    data = json.load(f)

    lista_pagos_actualizados = []
    # Recorro los listPagos
    for listPago in data['boleta']['listPagos']:
        if listPago['idTipoObligacion'][:3] not in ('AUT', 'EMB'):
            pago_actualizado = listPago
        else:
            # Hacer algo con la cabecera
            df_formulas_calc = df_formulas[df_formulas['FOR_IMPUESTO'] == listPago['idTipoObligacion'][:3]]
            df_detalles_pago = pd.DataFrame(listPago['listDetallesPago'])

            # extraigo de la caracteristica el tipo de mov
            # df_detalles_pago['tipo_mov'] = df_detalles_pago['caracteristica'].apply(lambda x: x['valor'])
            # detalles_pago = df_detalles_pago.drop("caracteristica", axis=1)
            df_detalles_pago = pd.json_normalize(listPago['listDetallesPago'], 'caracteristica',
                                                 ['idTransaccion', 'tipoMovimiento', 'montoPagado'])
            df_detalles_pago["montoPagado"] = pd.to_numeric(df_detalles_pago["montoPagado"])
            df_detalles_pago = pd.merge(df_detalles_pago, df_formulas_calc, left_on='valor', right_on='FOR_CPTO_ORIGEN')
            # Calculo el monto de recargos y descuentos
            # indice_general = df_detalles_pago[df_detalles_pago['FOR_CPTO_CALCULO'] == 'GENERAL'].index[0]
            # monto_recargo = df_detalles_pago.loc[indice_general, 'montoPagado']
            monto_recargo = df_detalles_pago[df_detalles_pago['FOR_CPTO_CALCULO'] == 'GENERAL'].groupby('FOR_CPTO_CALCULO')['montoPagado'].sum().values[0]

            # Reducimos el dataframe a la suma de los conceptos
            groupby_cols = ['tipo', 'FOR_IMPUESTO',  'FOR_CPTO_CALCULO','FOR_FORMULA_CALCULO', 'FOR_PRORRATEA', 'FOR_VIGENCIA_DESDE', 'FOR_VIGENCIA_HASTA', 'FOR_SALIDA_JSON']
            df_suma_conceptos = df_detalles_pago.groupby(groupby_cols)['montoPagado'].sum().reset_index()

            # Calcula porcentaje de GENERAL (Redondeo) si se distribuye
            existe_autmun = 'AUTMUN' in df_detalles_pago['FOR_CPTO_ORIGEN'].values
            # existe_general = 'GENERAL' in df_calculo['FOR_CPTO_CALCULO'].values
            prorratea = 'S' in df_detalles_pago['FOR_PRORRATEA'].values

            if existe_autmun and prorratea:
                df_suma_conceptos['montoRecargo'] = monto_recargo * .5
            else:
                df_suma_conceptos['montoRecargo'] = monto_recargo

            df = df_suma_conceptos

            # Aplico formula para calculo de monto final
            # pd.options.mode.chained_assignment = None
            df_list = []
            for formula in df_suma_conceptos['FOR_FORMULA_CALCULO'].unique():
                df_new = df_suma_conceptos[df_suma_conceptos['FOR_FORMULA_CALCULO'] == formula].copy()
                df_new['montoCalculado'] = pd.eval(formula)
                df_list.append(df_new)
            df_calculo = pd.concat(df_list, ignore_index=True)

            #df_calculo = df_calculo.groupby('FOR_CPTO_CALCULO')['montoCalculado'].sum().reset_index()
            df_calculo = df_calculo.drop(columns=['montoPagado'])
            calculado = df_calculo.rename(columns={'FOR_CPTO_CALCULO': 'tipoMovimiento', 'montoCalculado': 'montoPagado'})
            calculado['montoPagado'] = calculado['montoPagado'].round(2)

            v_salida = calculado
            v_salida['idTransaccion'] = datetime.now().strftime('%y%m%d%H%M%S%f')[:14]
            v_salida['caracteristica'] = v_salida['tipoMovimiento'].apply(lambda x: f"[{{'tipo': 'CMCCCDC', 'valor':'{x}'}}]")
            v_salida = v_salida[(v_salida['montoPagado'] > 0) & (v_salida['FOR_SALIDA_JSON'] == 'S')]
            v_salida = v_salida[['tipoMovimiento','montoPagado','idTransaccion','caracteristica']]

            pago_actualizado = listPago
            pago_actualizado['listDetallesPago'] = v_salida.to_dict('records')

        lista_pagos_actualizados.append(pago_actualizado)

    boleta_nueva = data['boleta']
    boleta_nueva['listPagos'] = lista_pagos_actualizados

    #with open("outputAUTbyLeo.json", "w") as outfile:
        #    # with open("outputEMB.json", "w") as outfile:
    #    json.dump({'boleta': boleta_nueva}, outfile, indent=4)
    print({'boleta': boleta_nueva})


if __name__ == "__main__":
    with open('Automotor3.json', encoding="utf8") as f:
        data = json.load(f)
    main(data)
