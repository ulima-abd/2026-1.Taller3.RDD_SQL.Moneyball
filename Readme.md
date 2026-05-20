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

---

## BLOQUE A — Exploración Básica con RDD

---

### Pregunta 1 · RDD — Identificación del "Mercado Ineficiente" (Filtrado de Agentes Libres)

La primera regla de Moneyball es encontrar valor donde otros no ven nada. Los jugadores sin club o con valor de transferencia cero (0$) son activos sin costo de adquisición. Para dimensionar el tamaño de nuestro mercado ineficiente, construya un flujo en Spark que filtre el dataset y devuelva el conteo total de jugadores que están tasados exactamente en **"0$"** (Transfer Value) y que actualmente pertenezcan a algún club (es decir, que el campo Club no esté vacío ni sea "-").

---

### Pregunta 2 · RDD — Buscando al "Sustituto de la Estrella" en el Medio Campo

Hemos vendido a nuestro centrocampista estrella. No podemos comprar otro igual, pero podemos buscar un jugador joven, barato (valor "0$") y con una visión de juego excelsa (Vis ≥ 15) para que replique su capacidad de crear jugadas.

Encuentre cuántos centrocampistas (Position contiene la letra "M") cumplen con el perfil Moneyball:

- Edad menor a **23 años**
- Atributo de visión (Vis) mayor o igual a **15**
- Valor de transferencia **"0$"**

---

### Pregunta 3 · RDD — El Factor Mental – El Líder Barato por Excelencia

El liderazgo (Ldr) suele pagarse muy caro en el mercado tradicional. Queremos demostrar estadísticamente que existen jugadores con un liderazgo altísimo que se pueden adquirir a costo cero.

Genere un RDD que identifique a los jugadores con valor "0$" que tengan un Liderazgo (Ldr) máximo de 20. Muestre el **top 5** de estos jugadores (Nombre, Club y su valor de Liderazgo) ordenados **alfabéticamente por su nombre** en caso de empate.

---

### Pregunta 4 · RDD — Mapeo de Canteras Eficientes (Suma de Atributos por País)

Queremos saber en qué países se concentran los jugadores con mejor técnica (Tec) pero que se mantienen con un valor de mercado de "0$", para enviar allá a nuestros analistas.

Calcule la puntuación total acumulada de Técnica de todos los jugadores con valor "0$" agrupados por su nacionalidad (Nat). Ordene los resultados de mayor a menor y extraiga las **5 nacionalidades** con mayor "técnica barata" acumulada.

---

### Pregunta 5 · RDD — Análisis de Costo-Beneficio (Liderazgo Promedio por Club)

Para medir qué tan bien gestionan los clubes el "Liderazgo" de sus plantillas sin gastar de más, la directiva quiere evaluar el promedio de esta métrica en los jugadores que son catalogados como "activos sin costo" (0$).

Calcule el **promedio del atributo de Liderazgo (Ldr)** de los jugadores con valor de transferencia "0$" para cada Club (excluyendo clubes vacíos o "-"). La salida debe ser una lista de tuplas `(Club, Liderazgo_Promedio)`.

---

### Pregunta 6 · RDD — Distribución de Edad del Mercado Ineficiente (Agregación Compleja)

El valor de reventa futuro es vital. Un jugador gratis y joven es una inversión perfecta; un jugador gratis pero muy viejo puede ser un riesgo físico. Necesitamos entender la estructura de edad de nuestro mercado objetivo.

Agrupe a todos los jugadores cuyo valor de transferencia es "0$" según su rango de edad en dos grupos:

- **"Jóvenes (Menores de 25)"**
- **"Veteranos (25 o más)"**

Calcule el conteo total de jugadores y la técnica promedio (Tec) de cada grupo.

---

## BLOQUE B — Análisis Avanzado con Spark SQL

---

### Pregunta 7 · RDD — Radar de Talento Integral – Score Compuesto de Potencial Moneyball

El director técnico exige un solo número que resuma la "ganga" de cada jugador. El analista jefe ha propuesto un Score Compuesto que pondera las tres métricas clave de forma diferente según su importancia táctica:

> **Fórmula del Score Compuesto**
>
> $$\text{Score} = (\text{Vis} \times 0.40) + (\text{Tec} \times 0.35) + (\text{Ldr} \times 0.25)$$
>
> *Lógica: la Visión de juego (40%) aporta más en la construcción táctica; la Técnica (35%) determina la calidad de ejecución; el Liderazgo (25%) asegura cohesión grupal. La suma de pesos es siempre 1.0.*

Usando RDD, filtre únicamente los jugadores con Transfer Value "0$" y calcule este Score para cada uno. Muestre el **Top 10** de jugadores con mayor Score Compuesto. La salida debe incluir: Nombre, Club, Posición y Score (redondeado a 2 decimales), ordenados de mayor a menor score.

> **Pista de exploración:** ¿Qué posición domina el top 10? ¿Los delanteros (ST/CF) o los mediocampistas (M)? ¿Existe algún correlato entre club de origen y score alto? Plantea una hipótesis antes de ejecutar y contrástala con el resultado.

---

### Pregunta 8 · SQL — Análisis de Brecha por Posición – ¿Dónde hay más talento oculto?

El scout jefe sospecha que ciertas posiciones concentran una cantidad desproporcionada de talento gratuito. Para confirmar su hipótesis necesita una tabla comparativa.

Convierte el RDD en un DataFrame de Spark y registra una vista temporal.

```python
# Convertir a DataFrame asignando nombres de columnas
df = rdd.toDF(["Nombre", "Edad"])

# Mostrar el resultado
df.show()
```

Usando Spark SQL, calcula para cada Posición (Position) de los jugadores con Transfer Value "0$":

- **Cantidad total de jugadores** con ese perfil (`COUNT`)
- **Visión promedio** (`AVG Vis`)
- **Técnica promedio** (`AVG Tec`)
- **Liderazgo promedio** (`AVG Ldr`)

**Condición:** Solo incluya posiciones con **más de 10 jugadores** en el mercado gratuito (usa `HAVING`). Ordena los resultados por Técnica promedio de forma descendente.

> **Desafío adicional – Plantea tu estrategia antes de codificar:**
>
> Antes de escribir la consulta SQL responde:
> 1. ¿Por qué el umbral de 10 jugadores ayuda a evitar conclusiones espurias?
> 2. ¿Qué posición esperas que tenga la mayor técnica promedio y por qué?
> 3. Si el resultado contradice tu hipótesis, ¿cómo ajustarías el modelo de scouting?

---

### Pregunta 9 · SQL — Detección de Monopolios de Talento – Clubes con Reservas Ocultas

La directiva sospecha que algunos clubes acumulan a propósito jugadores con valor 0$ para luego venderlos renovados. Queremos identificar esos "monopolizadores de talento" antes de que lo hagan.

Usando Spark SQL, identifica los clubes que simultáneamente cumplen:

- Tienen **más de 5 jugadores** con Transfer Value "0$"
- Al menos uno de esos jugadores tiene **Liderazgo ≥ 18**
- El **promedio de Técnica** del grupo supera **14**

Para cada club detectado, muestra: nombre del Club, total de jugadores gratuitos, Técnica promedio y el Liderazgo máximo del grupo. Ordena por total de jugadores gratuitos de forma descendente.

> **Pista metodológica:** Puedes utilizar subconsultas o CTE (`WITH ...`) para primero calcular los agregados por club y luego aplicar los filtros de la cláusula `HAVING`. Esto mejora la legibilidad y el rendimiento del plan de ejecución en Spark.

---

## BLOQUE C — Integración y Estrategia

---

### Pregunta 10 · RDD — Índice de Desempeño Relativo por Nacionalidad – ¿Quién está por encima de la media?

No basta saber que un jugador tiene buena técnica en términos absolutos; lo que importa es si supera la media de su propia nacionalidad. Un jugador con Tec = 16 en un grupo cuya media es 13 es más valioso que uno con Tec = 16 en un grupo cuya media es 17.

Filtra y muestra solo los jugadores que están en el **Top 3 de su propia nación** y que además tengan una diferencia positiva respecto a la media. Limita la salida a las **5 naciones con mayor promedio nacional de Técnica** (entre los que cumplen los filtros anteriores).

---

### Pregunta 11 · RDD + SQL — Estrategia de Fichar por Perfil Mixto – Pipeline Completo de Decisión

La directiva necesita presentar al consejo una lista ejecutiva final de fichajes recomendados. No se trata solo de un número o de una tabla: requiere un pipeline reproducible que combine el Score Compuesto (Pregunta 7) con el contexto de mercado (Preguntas 8 y 9) para producir una lista priorizada y justificada.

**Instrucciones del pipeline:**

1. **Calcular el Score Compuesto con RDD** (igual que en P7) para todos los jugadores con Transfer Value "0$".
2. **Convertir el RDD resultante en un DataFrame** y registrarlo como vista temporal.
3. **Aplicar los siguientes filtros estratégicos con Spark SQL:**
   - Score Compuesto ≥ **12.0**
   - Edad entre **18 y 26 años** inclusive
   - Posición que contenga **"M"** (mediocampistas) o **"ST"** (delanteros)
   - Club válido (no vacío ni "-")
4. **Ordenar** los resultados por Score Compuesto de mayor a menor.
5. **Mostrar los primeros 15 candidatos** con: Nombre, Club, Posición, Edad, Score Compuesto.

> **Sección de Reflexión Estratégica — Obligatoria**
>
> Tras ejecutar el pipeline, responde en tu reporte:
>
> **A)** ¿Qué posición domina la lista final y qué dice eso sobre las ineficiencias del mercado?
>
> **B)** ¿Los candidatos provienen de clubes grandes o de clubes pequeños/medianos? Explica la implicancia táctica.
>
> **C)** Si tuvieras que ajustar los pesos del Score Compuesto para favorecer más el Liderazgo (equipo joven e indisciplinado), ¿cómo cambiaría la fórmula y qué efecto esperarías en la lista?
>
> **D)** ¿Qué limitación metodológica tiene este pipeline al no considerar el campo de juego (local/visitante) o la posición exacta dentro del esquema táctico?
