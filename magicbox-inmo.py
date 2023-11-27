import pandas as pd
import json
import os
from datetime import datetime


def main(data):
    estado = 'Procesando'
    df_formulas = pd.read_csv('formulas.csv', delimiter=';')

    # Separo formulas de calculo y distribucion
    df_formulas_calc = df_formulas.drop(["FORMULA_DISTRI", 'CTA_DESTINO'], axis=1)
    df_formulas_calc = df_formulas_calc.drop_duplicates(
        subset=['IMPUESTO', 'CPTO_ORIGEN', 'CPTO_CALCULO', 'CPTO_DISTRI', 'FORMULA_CALCULO', 'DISTRIBUYE'])
    df_formulas_distr = df_formulas.drop(["FORMULA_CALCULO"], axis=1)
    df_formulas_distr = df_formulas_distr.drop_duplicates(
        subset=['IMPUESTO', 'CPTO_ORIGEN', 'CPTO_CALCULO', 'CPTO_DISTRI', 'FORMULA_DISTRI', 'CTA_DESTINO',
                'DISTRIBUYE'])

    # Abro archivo JSON
    #with open('AutomotorVarios.json', encoding="utf8") as f:
    #    data = json.load(f)

    lista_pagos_actualizados = []
    # Recorro los listPagos
    for listPago in data['boleta']['listPagos']:
        # Hacer algo con la cabecera
        df_detalles_pago = pd.DataFrame(listPago['listDetallesPago'])

        # extraigo de la caracteristica el tipo de mov
        #df_detalles_pago['tipo_mov'] = df_detalles_pago['caracteristica'].apply(lambda x: x['valor'])
        #detalles_pago = df_detalles_pago.drop("caracteristica", axis=1)
        df_detalles_pago = pd.json_normalize(listPago['listDetallesPago'], 'caracteristica',['idTransaccion','tipoMovimiento','montoPagado'])
        df_detalles_pago["montoPagado"] = pd.to_numeric(df_detalles_pago["montoPagado"])
        df_detalles_pago = pd.merge(df_detalles_pago, df_formulas_calc, left_on='valor', right_on='CPTO_ORIGEN')
        # Calculo el monto de recargos y descuentos
        indice_general = df_detalles_pago[df_detalles_pago['CPTO_CALCULO'] == 'GENERAL'].index[0]
        monto_recargo = df_detalles_pago.loc[indice_general, 'montoPagado']

        # Calcula porcentaje de GENERAL (Redondeo) si se distribuye
        existe_autmun = 'AUTMUN' in df_detalles_pago['CPTO_ORIGEN'].values
        # existe_general = 'GENERAL' in df_calculo['CPTO_CALCULO'].values
        distribuye = 'S' in df_detalles_pago['DISTRIBUYE'].values

        if existe_autmun and distribuye:
            df_detalles_pago['montoRecargo'] = monto_recargo * .5
        else:
            df_detalles_pago['montoRecargo'] = monto_recargo

        df = df_detalles_pago

        # Separo recargos de conceptos validos
        df_recargo = df_detalles_pago[df_detalles_pago['CPTO_CALCULO'] == 'GENERAL']
        df_calculo = df_detalles_pago[df_detalles_pago['CPTO_CALCULO'] != 'GENERAL']

        # Aplico formula para calculo de monto final
        #pd.options.mode.chained_assignment = None
        df_list = []
        for formula in df_calculo['FORMULA_CALCULO'].unique():
            df_new = df_calculo[df_calculo['FORMULA_CALCULO'] == formula].copy()
            df_new['montoCalculado'] = pd.eval(formula)
            df_list.append(df_new)
        df_calculo = pd.concat(df_list, ignore_index=True)

        df_calculo = df_calculo.groupby('CPTO_CALCULO')['montoCalculado'].sum().reset_index()
        calculado = df_calculo.rename(columns={'CPTO_CALCULO': 'tipoMovimiento', 'montoCalculado': 'montoPagado'})
        calculado['montoPagado'] = calculado['montoPagado'].round(2)

        v_salida = calculado
        v_salida[
            'idTransaccion'] = f"{datetime.now().year % 100}{datetime.now().month:02}{datetime.now().day:02}{datetime.now().hour:02}{datetime.now().minute:02}{datetime.now().second:02}{datetime.now().microsecond // 10000:02}"
        v_salida['caracteristica'] = '[{''tipo'': ''CMCCCDC'', ''valor'':'' }]'

        pago_actualizado = listPago
        pago_actualizado['listDetallesPago'] = v_salida.to_dict('records')

        lista_pagos_actualizados.append(pago_actualizado)

    boleta_nueva = data['boleta']
    boleta_nueva['listPagos'] = lista_pagos_actualizados

    with open("outputAUTbyLeo.json", "w") as outfile:
    #    # with open("outputEMB.json", "w") as outfile:
        json.dump({'boleta':boleta_nueva}, outfile, indent=4)
    return {'boleta':boleta_nueva}



if __name__ == "__main__":
    with open('Varios.json', encoding="utf8") as f:
       data = json.load(f)
    main(data)