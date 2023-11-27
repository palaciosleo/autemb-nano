import pandas as pd
import json
import os
import cx_Oracle
from datetime import datetime
from configparser import ConfigParser

def main(data):
    # config = ConfigParser()
    # config.read('configuracion_db-DESA.ini')
    #
    # # Obtener la información de conexión desde la configuración
    # host = config['DATABASE']['host']
    # port = int(config['DATABASE']['port'])
    # sid = config['DATABASE']['sid']
    # user = config['DATABASE']['user']
    # password = config['DATABASE']['password']
    #
    # service_name = cx_Oracle.makedsn(host, port, service_name=sid)
    # connection = cx_Oracle.connect(user, password, service_name, encoding="UTF-8")
    # cursor = connection.cursor()
    # cursor.execute('SELECT FOR_IMPUESTO, FOR_CPTO_ORIGEN, FOR_CPTO_CALCULO, FOR_FORMULA_CALCULO,  FOR_PRORRATEA, FOR_VIGENCIA_DESDE, FOR_VIGENCIA_HASTA, FOR_SALIDA_JSON, FOR_SUB_TIPO FROM FORMULAS WHERE FOR_FECHA_BAJA IS NULL')
    # result_set = cursor.fetchall()
    # df_formulas = pd.DataFrame(result_set, columns=[col[0] for col in cursor.description])
    #
    # cursor.close()
    # connection.close()
    df_formulas = pd.read_csv('formulas_20232711.csv', delimiter=';')
    df_formulas_calc = df_formulas
    # Separo formulas de calculo y distribucion
    df_formulas_calc = df_formulas_calc.drop_duplicates(subset=['FOR_IMPUESTO', 'FOR_CPTO_ORIGEN', 'FOR_CPTO_CALCULO', 'FOR_FORMULA_CALCULO', 'FOR_PRORRATEA'])

    # Abro archivo JSON
    #with open('AutomotorVarios.json', encoding="utf8") as f:
    #    data = json.load(f)

    lista_pagos_actualizados = []
    # Recorro los listPagos
    for listPago in data['boleta']['listPagos']:
        if listPago['idTipoObligacion'][:3] not in ('AUT', 'EMB', 'INM'):
            pago_actualizado = listPago
        elif listPago['idTipoObligacion'][:3] == 'INM':
            df_sub_tipo = df_formulas[df_formulas['FOR_SUB_TIPO'].notnull()][['FOR_CPTO_ORIGEN', 'FOR_SUB_TIPO']]
            df_formulas_calc = df_formulas
            df_detalles_pago = pd.DataFrame(listPago['listDetallesPago'])

            # extraigo de la caracteristica el tipo de mov
            df_detalles_pago = pd.json_normalize(listPago['listDetallesPago'], 'caracteristica', ['idTransaccion', 'tipoMovimiento', 'montoPagado'])
            # df_detalles_pago['tipo_mov'] = df_detalles_pago['caracteristica'].apply(lambda x: x['valor'])
            # detalles_pago = df_detalles_pago.drop("caracteristica", axis=1)
            v_filtro_imp = pd.merge(df_sub_tipo, df_detalles_pago, left_on='FOR_CPTO_ORIGEN', right_on='valor', how='inner')
            v_filtro_imp = v_filtro_imp['FOR_SUB_TIPO'].to_string(index=False)
            df_formulas_calc = df_formulas[df_formulas['FOR_IMPUESTO'] == v_filtro_imp]
            df_final = pd.merge(df_formulas_calc, df_detalles_pago, how='left', left_on='FOR_CPTO_ORIGEN',
                                right_on='valor').fillna({'montoPagado': 0})
            df_final['tipo_mov'] = df_final['valor'].fillna(df_final['FOR_CPTO_ORIGEN'])
            df_final['tipoMovimiento'] = df_final['tipoMovimiento'].fillna(df_final['FOR_CPTO_ORIGEN'])

            df_calculo = df_final
            df_calculo2 = df_calculo[['idTransaccion', 'FOR_CPTO_CALCULO', 'FOR_FORMULA_CALCULO', 'FOR_SALIDA_JSON']]
            df_conceptos = df_calculo.groupby('FOR_CPTO_CALCULO')['montoPagado'].sum().reset_index()

            df_pivot = df_conceptos.pivot(columns='FOR_CPTO_CALCULO', values='montoPagado')
            df_pivot = df_pivot.fillna(0)
            total_row = df_pivot.sum(axis=0).to_frame().T

            df_result = pd.merge(df_calculo2, total_row, how='cross')
            df = df_result

            # Aplico formula para calculo de monto final
            pd.options.mode.chained_assignment = None
            df_list = []
            for formula in df['FOR_FORMULA_CALCULO'].unique():
                #    print(formula)
                df_new = df[df['FOR_FORMULA_CALCULO'] == formula].copy()
                df_new['montoCalculado'] = pd.eval(formula)
                df_list.append(df_new)
            df_calculo = pd.concat(df_list, ignore_index=True)

            v_salida = df_calculo
            # v_salida['idTransaccion'] = f"{datetime.now().year % 100}{datetime.now().month:02}{datetime.now().day:02}{datetime.now().hour:02}{datetime.now().minute:02}{datetime.now().second:02}{datetime.now().microsecond // 10000:02}"
            v_salida['caracteristica'] = v_salida['FOR_CPTO_CALCULO'].apply(
                lambda x: f"[{{'tipo': 'CMCCCDC', 'valor':'{x}'}}]")
            v_salida = v_salida[(v_salida['montoCalculado'] > 0) & (v_salida['FOR_SALIDA_JSON'] == 'S')]
            v_salida = v_salida.rename(columns={'FOR_CPTO_CALCULO': 'tipoMovimiento'})

            v_salida = v_salida[['tipoMovimiento', 'caracteristica', 'idTransaccion', 'montoCalculado']].round(
                {'montoCalculado': 2})

            pago_actualizado = listPago
            pago_actualizado['listDetallesPago'] = v_salida.to_dict('records')


        else:
            # Hacer algo con la cabecera
            df_formulas_calc = df_formulas[df_formulas['FOR_IMPUESTO'] == listPago['idTipoObligacion'][:3]]
            df_detalles_pago = pd.DataFrame(listPago['listDetallesPago'])

            # extraigo de la caracteristica el tipo de mov
            df_detalles_pago = pd.json_normalize(listPago['listDetallesPago'], 'caracteristica',
                                                 ['idTransaccion', 'tipoMovimiento', 'montoPagado'])
            df_detalles_pago["montoPagado"] = pd.to_numeric(df_detalles_pago["montoPagado"])
            df_detalles_pago = pd.merge(df_detalles_pago, df_formulas_calc, left_on='valor', right_on='FOR_CPTO_ORIGEN')
            monto_recargo = df_detalles_pago[df_detalles_pago['FOR_CPTO_CALCULO'] == 'GENERAL'].groupby('FOR_CPTO_CALCULO')['montoPagado'].sum().get(0, 0)

            # Reducimos el dataframe a la suma de los conceptos
            groupby_cols = ['tipo', 'FOR_IMPUESTO',  'FOR_CPTO_CALCULO','FOR_FORMULA_CALCULO', 'FOR_PRORRATEA', 'FOR_SALIDA_JSON']
            df_suma_conceptos = df_detalles_pago.groupby(groupby_cols)['montoPagado'].sum().reset_index()

            # Calcula porcentaje de GENERAL (Redondeo) si se distribuye
            existe_autmun = 'AUTMUN' in df_detalles_pago['FOR_CPTO_ORIGEN'].values
            prorratea = 'S' in df_detalles_pago['FOR_PRORRATEA'].values

            if existe_autmun and prorratea:
                df_suma_conceptos['montoRecargo'] = monto_recargo * .5
            else:
                df_suma_conceptos['montoRecargo'] = monto_recargo

            df = df_suma_conceptos

            # Aplico formula para calculo de monto final
            df_list = []
            for formula in df_suma_conceptos['FOR_FORMULA_CALCULO'].unique():
                df_new = df_suma_conceptos[df_suma_conceptos['FOR_FORMULA_CALCULO'] == formula].copy()
                df_new['montoCalculado'] = pd.eval(formula)
                df_list.append(df_new)
            df_calculo = pd.concat(df_list, ignore_index=True)

            df_calculo = df_calculo.drop(columns=['montoPagado'])
            calculado = df_calculo.rename(columns={'FOR_CPTO_CALCULO': 'tipoMovimiento', 'montoCalculado': 'montoPagado'})
            calculado['montoPagado'] = calculado['montoPagado'].round(2)

            v_salida = calculado
            v_salida['idTransaccion'] = f"{datetime.now().year % 100}{datetime.now().month:02}{datetime.now().day:02}{datetime.now().hour:02}{datetime.now().minute:02}{datetime.now().second:02}{datetime.now().microsecond // 10000:02}"
            v_salida['caracteristica'] = v_salida['tipoMovimiento'].apply(lambda x: f"[{{'tipo': 'CMCCCDC', 'valor':'{x}'}}]")
            v_salida = v_salida[(v_salida['montoPagado'] > 0) & (v_salida['FOR_SALIDA_JSON'] == 'S')]
            v_salida = v_salida[['tipoMovimiento','montoPagado','idTransaccion','caracteristica']]

            pago_actualizado = listPago
            pago_actualizado['listDetallesPago'] = v_salida.to_dict('records')

        lista_pagos_actualizados.append(pago_actualizado)

    boleta_nueva = data['boleta']
    boleta_nueva['listPagos'] = lista_pagos_actualizados

    with open("outputAUT.json", "w") as outfile:
        #    # with open("outputEMB.json", "w") as outfile:
        json.dump({'boleta': boleta_nueva}, outfile, indent=4)
    return {'boleta': boleta_nueva}


if __name__ == "__main__":
    with open('InmoURBPRO.json', encoding="utf8") as f:
        data = json.load(f)
    main(data)