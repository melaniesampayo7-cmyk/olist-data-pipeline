
# Meses del histórico de Olist que faltan procesar

$meses = @(
    "2016-12",
    "2017-04",
    "2017-05",
    "2017-06",
    "2017-07",
    "2017-08",
    "2017-09",
    "2017-10",
    "2017-11",
    "2017-12",
    "2018-01",
    "2018-02",
    "2018-03",
    "2018-04",
    "2018-05",
    "2018-06",
    "2018-07",
    "2018-08",
    "2018-09",
    "2018-10"
)

foreach ($mes in $meses) {
    Write-Host "Enviando mes histórico: $mes"

    
    # Construir JSON y escapar las comillas para Docker
    $configuracion = "{\`"business_month\`":\`"$mes\`"}"

    docker compose exec -T airflow-apiserver `
        airflow dags trigger olist_pipeline -c $configuracion


    if ($LASTEXITCODE -ne 0) {
        Write-Error "Error al enviar $mes. Se detiene el script."
        break
    }
}

Write-Host "Finalizado el envío de meses históricos."
