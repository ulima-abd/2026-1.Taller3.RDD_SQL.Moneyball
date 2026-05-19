# Taller Implementación de un Sistema de Moneyball con Spark RDD

## Contexto

El club está al borde de la quiebra técnica y no puede competir fichando superestrellas millonarias. Siguiendo la filosofía Moneyball, la directiva ha decidido despedir a los ojeadores tradicionales y confiar ciegamente en la analítica de datos. Tu objetivo como Ingeniero de Sistemas de Datos es procesar el dataset de la liga (merged_players.csv) mediante Apache Spark (API RDD) para identificar jugadores con métricas excepcionales pero cuyo valor de transferencia sea nulo o muy bajo.

## Instrucciones iniciales

Se le entregará un archivo llamado `merged_players.csv` con la información de los jugadores disponibles en el juego. Los campos a utilizar son los siguientes:

| Campo | Posición |
| ----- | -------- |
| Name  | 2 |
| Club | 6 |
| Nat | 8 |
| Age | 11 |
| Position | 12 |
| Transfer Value | 13 |
| Vis (Vision) | 29 |
| Tec (Técnica) | 31 |
| Ldr (Liderazgo) | 49 |

Para el presente taller, primero deberá de realizar las preguntas utilizando RDD y luego proseguir con Spark SQL (Dataframes).

Para cargar el CSV utilizando RDDs:

```python
import csv
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
raw_rdd = sc.textFile("merged_players.csv")

# 1. Extraer y aislar la cabecera
header = raw_rdd.first()

# 2. Filtrar cabecera y parsear cada línea de forma segura usando la librería csv
data_rdd = raw_rdd.filter(lambda line: line != header).map(lambda line: next(csv.reader([line])))
```

## Pregunta 1: Identificación del "Mercado Ineficiente" (Filtrado de Agentes Libres)

La primera regla de Moneyball es encontrar valor donde otros no ven nada. Los jugadores sin club o con valor de transferencia cero (0$) son activos sin costo de adquisición. Para dimensionar el tamaño de nuestro mercado ineficiente, construya un flujo en Spark que filtre el dataset y devuelva el conteo total de jugadores que están tasados exactamente en "0$" (Transfer Value) y que actualmente pertenezcan a algún club (es decir, que el campo Club no esté vacío ni sea "-").

## Pregunta 2: Buscando al "Sustituto de la Estrella" en el Medio Campo

Hemos vendido a nuestro centrocampista estrella. No podemos comprar otro igual, pero podemos buscar un jugador joven, barato (valor "0$") y con una visión de juego excelsa ($\text{Vis} \ge 15$) para que replique su capacidad de crear jugadas.

Encuentre cuántos centrocampistas (Position contiene la letra "M") cumplen con el perfil Moneyball: edad menor a $23$ años, atributo de visión (Vis) mayor o igual a $15$, y un valor de transferencia de "0$".

## Pregunta 3: El Factor Mental – El Líder Barato por Excelencia

El liderazgo (Ldr) suele pagarse muy caro en el mercado tradicional. Queremos demostrar estadísticamente que existen jugadores con un liderazgo altísimo que se pueden adquirir a costo cero.

Genere un RDD que identifique a los jugadores con valor "0$" que tengan un Liderazgo (Ldr) máximo de $20$. Muestre el top 5 de estos jugadores (Nombre, Club y su valor de Liderazgo) ordenados alfabéticamente por su nombre en caso de empate.

## Pregunta 4: Mapeo de Canteras Eficientes (Suma de Atributos por País)

Queremos saber en qué países se concentran los jugadores con mejor técnica (Tec) pero que se mantienen con un valor de mercado de "0$", para enviar allá a nuestros analistas.

Calcule la puntuación total acumulada de Técnica de todos los jugadores con valor "0$" agrupados por su nacionalidad (Nat). Ordene los resultados de mayor a menor y extraiga las 5 nacionalidades con mayor "técnica barata" acumulada.

## Pregunta 5: Análisis de Costo-Beneficio (Liderazgo Promedio por Club)

Para medir qué también gestionan los clubes el "Liderazgo" de sus plantillas sin gastar de más, la directiva quiere evaluar el promedio de esta métrica en los jugadores que son catalogados como "activos sin costo" (0$).

Calcule el promedio del atributo de Liderazgo (Ldr) de los jugadores con valor de transferencia "0$" para cada Club (excluyendo clubes vacíos o "-"). La salida debe ser una lista de tuplas (Club, Liderazgo_Promedio).

## Pregunta 6: Distribución de Edad del Mercado Ineficiente (Agregación Compleja)

El valor de reventa futuro es vital. Un jugador gratis y joven es una inversión perfecta; un jugador gratis pero muy viejo puede ser un riesgo físico. Necesitamos entender la estructura de edad de nuestro mercado objetivo.

Agrupe a todos los jugadores cuyo valor de transferencia es "0$" según su rango de edad. Para simplificarlo en RDD, clasifíquelos en dos grupos: "Jóvenes (Menores de 25)" y "Veteranos (25 o más)". Calcule el conteo total de jugadores y la técnica promedio (Tec) de cada grupo.