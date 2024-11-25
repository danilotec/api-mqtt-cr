from flask import Flask, jsonify
from paho.mqtt.client import Client
import threading, json

app = Flask(__name__)

dados = {
    "hospital": "Hospital das clinicas",
    "pressão": None,
    "dew point": None,
    "oxigenio": None,
    "compressor 1": None,
    "compressor 2": None,
    "usina": None,
    "RFF": None,
    "BE": None
    }

def on_connect(client, userdata, flags, rc):
    print(f'rc: {rc}')

    topics = [
        ("Hclinicas_c1_179", 0),
        ("Hclinicas_c2_179", 0),
        ("Hclinicas_PO_179", 0),
        ("Hclinicas_ar comprimido_179", 0),
    ]
    client.subscribe(topics)
    print('Inscrito nos topicos: ', [t[0] for t in topics])
    

def on_message(client, userdata, msg):
    global dados
    try:
       
        data = msg.payload
        decode_data = json.loads(data.decode('utf-8'))
        value_str = decode_data.get("d", {})

        if 'D110' in value_str:
            float_value = float(value_str['D110']) / 10
            dados['pressão'] = float_value
        if 'PO' in value_str:
            int_value = int(value_str['PO']) - 64850
            dados['dew point'] = int_value
        if 'Y0' in value_str:
            if value_str['Y0'] == '1':
                dados['compressor 1'] = 'Ligado'
            elif value_str['Y0'] == '0':
                dados['compressor 1'] = 'stand-by'
        if 'M240' in value_str:
            if value_str['M240'] == '1':
                dados['compressor 2'] = 'Ligado'
            elif value_str['M240'] == '0':
                dados['compressor 2'] = 'stand-by'

    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
    except KeyError as e:
        print(f"Chave ausente no dicionário: {e}")
    except ValueError as e:
        print(f"Erro ao converter valor: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")
        

def setup_mqtt():
    client = Client()
    client.on_message = on_message
    client.on_connect = on_connect
    client.connect('broker.hivemq.com', 1883, 60)

    threading.Thread(target=client.loop_forever).start()


@app.route('/')
def index():
    return 'API MQTT MONITOR'

@app.route('/data', methods=['GET', 'POST'])
def get_data():
    return jsonify(dados)

setup_mqtt()

if __name__ == '__main__':
    app.run(debug=True)
