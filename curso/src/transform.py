import json
from datetime import datetime
from logger import get_logger

log = get_logger(__name__)


def parse_payload(message):
    try:
        payload = message.decode()
        dados = json.loads(payload)
    except json.JSONDecodeError:
        raise ValueError('Integridade Falhou: arquivo JSON inválido')

    resultado = {}
    resultado.update(verifica_metadados(dados))
    resultado['orbital']        = verifica_orbital(dados.get('telemetry', {}).get('orbital', {}))
    resultado['power']          = verifica_power(dados.get('telemetry', {}).get('power', {}))
    resultado['thermal']        = verifica_thermal(dados.get('telemetry', {}).get('thermal', {}))
    resultado['propulsion']     = verifica_propulsion(dados.get('telemetry', {}).get('propulsion', {}))
    resultado['communications'] = verifica_communications(dados.get('telemetry', {}).get('communications', {}))
    resultado['payload_sensors'] = verifica_payload_sensors(
        dados.get('payload_sensors', {}),
        resultado['orbital']['latitude']
    )
    resultado['diagnostics']    = verifica_diagnostics(dados.get('diagnostics', {}))
    return resultado


# ---------------------------------------------------------------------------
# Metadados
# ---------------------------------------------------------------------------

def verifica_metadados(dados):
    # Verificação de presença
    var_obrigatorias = ['timestamp', 'satellite_id', 'health_status']
    for var in var_obrigatorias:
        if var not in dados:
            raise ValueError(f'Integridade Falhou: variável {var} não recebida pelo sensor')

    # Verificação de domínio
    dominio_status = ['OPTIMAL', 'DEGRADING', 'CRITICAL', 'MAINTENANCE']
    status_recebido = dados['health_status']
    if status_recebido not in dominio_status:
        raise ValueError(f'Integridade Falhou: status {status_recebido} não reconhecido no domínio')

    # Verificação de tipo
    try:
        timestamp_formatado = datetime.fromisoformat(dados['timestamp'])
    except Exception:
        raise ValueError(f"Integridade Falhou: formato datetime incorreto '{dados['timestamp']}'")

    return {
        'timestamp':    timestamp_formatado,
        'satellite_id': dados['satellite_id'],
        'health_status': status_recebido,
    }


# ---------------------------------------------------------------------------
# telemetry.orbital
# ---------------------------------------------------------------------------

def verifica_orbital(dados):
    campos = ['latitude', 'longitude', 'altitude_km', 'velocity_kms']
    for campo in campos:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo orbital {campo} ausente')

    latitude     = dados['latitude']
    longitude    = dados['longitude']
    altitude_km  = dados['altitude_km']
    velocity_kms = dados['velocity_kms']

    if not isinstance(latitude, (int, float)):
        raise TypeError(f'Integridade Falhou: latitude deve ser numérico, recebido {type(latitude).__name__}')
    if not -90.0 <= latitude <= 90.0:
        raise ValueError(f'Integridade Falhou: latitude fora do domínio [-90, 90]: {latitude}')

    if not isinstance(longitude, (int, float)):
        raise TypeError(f'Integridade Falhou: longitude deve ser numérico, recebido {type(longitude).__name__}')
    if not -180.0 <= longitude <= 180.0:
        raise ValueError(f'Integridade Falhou: longitude fora do domínio [-180, 180]: {longitude}')

    if not isinstance(altitude_km, (int, float)):
        raise TypeError(f'Integridade Falhou: altitude_km deve ser numérico, recebido {type(altitude_km).__name__}')
    if altitude_km <= 0:
        raise ValueError(f'Integridade Falhou: altitude_km deve ser positivo: {altitude_km}')

    if not isinstance(velocity_kms, (int, float)):
        raise TypeError(f'Integridade Falhou: velocity_kms deve ser numérico, recebido {type(velocity_kms).__name__}')
    if velocity_kms <= 0:
        raise ValueError(f'Integridade Falhou: velocity_kms deve ser positivo: {velocity_kms}')

    return {
        'latitude':     float(latitude),
        'longitude':    float(longitude),
        'altitude_km':  float(altitude_km),
        'velocity_kms': float(velocity_kms),
    }


# ---------------------------------------------------------------------------
# telemetry.power
# ---------------------------------------------------------------------------

def verifica_power(dados):
    campos = ['battery_level_pct', 'solar_voltage_v', 'current_draw_a']
    for campo in campos:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo power {campo} ausente')

    battery     = dados['battery_level_pct']
    solar_v     = dados['solar_voltage_v']
    current_a   = dados['current_draw_a']

    if not isinstance(battery, (int, float)):
        raise TypeError(f'Integridade Falhou: battery_level_pct deve ser numérico, recebido {type(battery).__name__}')
    if not 0.0 <= battery <= 100.0:
        raise ValueError(f'Integridade Falhou: battery_level_pct fora do domínio [0, 100]: {battery}')

    if not isinstance(solar_v, (int, float)):
        raise TypeError(f'Integridade Falhou: solar_voltage_v deve ser numérico, recebido {type(solar_v).__name__}')
    if solar_v < 0:
        raise ValueError(f'Integridade Falhou: solar_voltage_v não pode ser negativo: {solar_v}')

    if not isinstance(current_a, (int, float)):
        raise TypeError(f'Integridade Falhou: current_draw_a deve ser numérico, recebido {type(current_a).__name__}')
    if current_a < 0:
        raise ValueError(f'Integridade Falhou: current_draw_a não pode ser negativo: {current_a}')

    return {
        'battery_level_pct': float(battery),
        'solar_voltage_v':   float(solar_v),
        'current_draw_a':    float(current_a),
    }


# ---------------------------------------------------------------------------
# telemetry.thermal
# ---------------------------------------------------------------------------

def verifica_thermal(dados):
    campos = ['core_temp_c', 'external_temp_c', 'cooling_active']
    for campo in campos:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo thermal {campo} ausente')

    core_temp     = dados['core_temp_c']
    external_temp = dados['external_temp_c']
    cooling       = dados['cooling_active']

    if not isinstance(core_temp, (int, float)):
        raise TypeError(f'Integridade Falhou: core_temp_c deve ser numérico, recebido {type(core_temp).__name__}')
    if not -100.0 <= core_temp <= 200.0:
        raise ValueError(f'Integridade Falhou: core_temp_c fora do domínio [-100, 200]: {core_temp}')

    if not isinstance(external_temp, (int, float)):
        raise TypeError(f'Integridade Falhou: external_temp_c deve ser numérico, recebido {type(external_temp).__name__}')
    if not -273.15 <= external_temp <= 300.0:
        raise ValueError(f'Integridade Falhou: external_temp_c fora do domínio físico: {external_temp}')

    if not isinstance(cooling, bool):
        raise TypeError(f'Integridade Falhou: cooling_active deve ser booleano, recebido {type(cooling).__name__}')

    return {
        'core_temp_c':     float(core_temp),
        'external_temp_c': float(external_temp),
        'cooling_active':  cooling,
    }


# ---------------------------------------------------------------------------
# telemetry.propulsion
# ---------------------------------------------------------------------------

def verifica_propulsion(dados):
    campos = ['fuel_remaining_kg', 'thruster_status']
    for campo in campos:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo propulsion {campo} ausente')

    fuel   = dados['fuel_remaining_kg']
    status = dados['thruster_status']

    if not isinstance(fuel, (int, float)):
        raise TypeError(f'Integridade Falhou: fuel_remaining_kg deve ser numérico, recebido {type(fuel).__name__}')
    if fuel < 0:
        raise ValueError(f'Integridade Falhou: fuel_remaining_kg não pode ser negativo: {fuel}')

    dominio_thruster = ['OK', 'WARNING', 'ERROR']
    if status not in dominio_thruster:
        raise ValueError(f'Integridade Falhou: thruster_status {status} não reconhecido no domínio')

    return {
        'fuel_remaining_kg': float(fuel),
        'thruster_status':   status,
    }


# ---------------------------------------------------------------------------
# telemetry.communications
# ---------------------------------------------------------------------------

def verifica_communications(dados):
    campos = ['signal_strength_dbm', 'packet_loss_pct', 'uplink_active']
    for campo in campos:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo communications {campo} ausente')

    signal      = dados['signal_strength_dbm']
    packet_loss = dados['packet_loss_pct']
    uplink      = dados['uplink_active']

    if not isinstance(signal, int):
        raise TypeError(f'Integridade Falhou: signal_strength_dbm deve ser inteiro, recebido {type(signal).__name__}')

    if not isinstance(packet_loss, (int, float)):
        raise TypeError(f'Integridade Falhou: packet_loss_pct deve ser numérico, recebido {type(packet_loss).__name__}')
    if not 0.0 <= packet_loss <= 100.0:
        raise ValueError(f'Integridade Falhou: packet_loss_pct fora do domínio [0, 100]: {packet_loss}')

    if not isinstance(uplink, bool):
        raise TypeError(f'Integridade Falhou: uplink_active deve ser booleano, recebido {type(uplink).__name__}')

    return {
        'signal_strength_dbm': signal,
        'packet_loss_pct':     float(packet_loss),
        'uplink_active':       uplink,
    }


# ---------------------------------------------------------------------------
# payload_sensors
# ---------------------------------------------------------------------------

def verifica_payload_sensors(dados, latitude):
    campos_obrigatorios = [
        'soil_moisture_index', 'surface_water_area_sqkm',
        'algae_bloom_index', 'sea_surface_salinity_psu',
    ]
    for campo in campos_obrigatorios:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo payload_sensors {campo} ausente')

    soil_moisture   = dados['soil_moisture_index']    # pode ser null/None
    surface_water   = dados['surface_water_area_sqkm']
    algae_bloom     = dados['algae_bloom_index']
    salinity        = dados['sea_surface_salinity_psu']
    ice_thickness   = dados.get('ice_thickness_m')    # opcional, apenas perto dos polos

    # soil_moisture_index: 0.0–1.0 ou null
    if soil_moisture is not None:
        if not isinstance(soil_moisture, (int, float)):
            raise TypeError(f'Integridade Falhou: soil_moisture_index deve ser numérico ou null, recebido {type(soil_moisture).__name__}')
        if not 0.0 <= soil_moisture <= 1.0:
            raise ValueError(f'Integridade Falhou: soil_moisture_index fora do domínio [0, 1]: {soil_moisture}')

    if not isinstance(surface_water, int):
        raise TypeError(f'Integridade Falhou: surface_water_area_sqkm deve ser inteiro, recebido {type(surface_water).__name__}')
    if surface_water < 0:
        raise ValueError(f'Integridade Falhou: surface_water_area_sqkm não pode ser negativo: {surface_water}')

    if not isinstance(algae_bloom, (int, float)):
        raise TypeError(f'Integridade Falhou: algae_bloom_index deve ser numérico, recebido {type(algae_bloom).__name__}')
    if not 0.0 <= algae_bloom <= 100.0:
        raise ValueError(f'Integridade Falhou: algae_bloom_index fora do domínio [0, 100]: {algae_bloom}')

    if not isinstance(salinity, (int, float)):
        raise TypeError(f'Integridade Falhou: sea_surface_salinity_psu deve ser numérico, recebido {type(salinity).__name__}')
    if salinity < 0:
        raise ValueError(f'Integridade Falhou: sea_surface_salinity_psu não pode ser negativo: {salinity}')

    # ice_thickness_m: somente registra próximo dos polos (|lat| > 60)
    if ice_thickness is not None:
        if not isinstance(ice_thickness, (int, float)):
            raise TypeError(f'Integridade Falhou: ice_thickness_m deve ser numérico, recebido {type(ice_thickness).__name__}')
        if ice_thickness < 0:
            raise ValueError(f'Integridade Falhou: ice_thickness_m não pode ser negativo: {ice_thickness}')
        if abs(latitude) <= 60:
            raise ValueError(f'Integridade Falhou: ice_thickness_m reportado fora das regiões polares (lat={latitude})')

    return {
        'soil_moisture_index':      soil_moisture,
        'surface_water_area_sqkm':  surface_water,
        'algae_bloom_index':        float(algae_bloom),
        'sea_surface_salinity_psu': float(salinity),
        'ice_thickness_m':          float(ice_thickness) if ice_thickness is not None else None,
    }


# ---------------------------------------------------------------------------
# diagnostics
# ---------------------------------------------------------------------------

def verifica_diagnostics(dados):
    campos = ['cpu_usage_pct', 'memory_usage_mb', 'last_error_code']
    for campo in campos:
        if campo not in dados:
            raise ValueError(f'Integridade Falhou: campo diagnostics {campo} ausente')

    cpu    = dados['cpu_usage_pct']
    memory = dados['memory_usage_mb']
    error  = dados['last_error_code']

    if not isinstance(cpu, int):
        raise TypeError(f'Integridade Falhou: cpu_usage_pct deve ser inteiro, recebido {type(cpu).__name__}')
    if not 0 <= cpu <= 100:
        raise ValueError(f'Integridade Falhou: cpu_usage_pct fora do domínio [0, 100]: {cpu}')

    if not isinstance(memory, int):
        raise TypeError(f'Integridade Falhou: memory_usage_mb deve ser inteiro, recebido {type(memory).__name__}')
    if memory < 0:
        raise ValueError(f'Integridade Falhou: memory_usage_mb não pode ser negativo: {memory}')

    if not isinstance(error, int):
        raise TypeError(f'Integridade Falhou: last_error_code deve ser inteiro, recebido {type(error).__name__}')
    if error < 0:
        raise ValueError(f'Integridade Falhou: last_error_code não pode ser negativo: {error}')

    return {
        'cpu_usage_pct':    cpu,
        'memory_usage_mb':  memory,
        'last_error_code':  error,
    }

