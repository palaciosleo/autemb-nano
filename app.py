from flask import Flask, request, jsonify
import json
import magicbox

app = Flask(__name__)

@app.route('/mi_api', methods=['POST'])
def mi_api():
    # Obtener el JSON del cuerpo de la solicitud
    data = request.get_json()

    # Llamar a magicbox.py con el JSON como argumento y capturar la salida
    try:
        response = magicbox.main(data)
    except Exception as e:
        response = {'error': 'Error al ejecutar magicbox.py', 'message': str(e)}

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)