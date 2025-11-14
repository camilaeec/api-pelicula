import boto3
import uuid
import os
import json
import traceback

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_entrada = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Evento de entrada recibido",
                "evento": event,
                "tenant_id": event['body']['tenant_id'],
                "accion": "crear_pelicula"
            }
        }
        print(json.dumps(log_entrada))
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Log de éxito
        log_exito = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "pelicula": pelicula,
                "response": str(response),
                "accion": "crear_pelicula"
            }
        }
        print(json.dumps(log_exito))
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
        
    except KeyError as e:
        # Log de error por clave faltante
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error de clave faltante en el evento",
                "error": str(e),
                "evento": event,
                "stack_trace": traceback.format_exc(),
                "accion": "crear_pelicula"
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'error': f'Clave faltante: {str(e)}'
        }
        
    except Exception as e:
        # Log de error genérico
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error inesperado al crear película",
                "error": str(e),
                "evento": event,
                "stack_trace": traceback.format_exc(),
                "accion": "crear_pelicula"
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'error': f'Error interno: {str(e)}'
        }
