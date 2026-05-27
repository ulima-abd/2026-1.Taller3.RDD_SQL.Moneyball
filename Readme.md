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

### Pregunta 7 · RDD — Diversidad Táctica del Mercado Gratuito (Distribución por Posición)

Antes de diseñar cualquier estrategia de fichajes, la dirección técnica necesita saber qué tipos de jugadores pueblan el mercado ineficiente. Conocer la distribución posicional permite identificar si hay superávit en alguna zona del campo que pueda aprovecharse tácticamente.

Construya un flujo RDD que, a partir del universo de jugadores con Transfer Value **"0$"** y club válido (no vacío ni "-"), calcule cuántos jugadores existen por cada valor único de **Position**. Muestre los resultados ordenados de **mayor a menor cantidad**, limitando la salida a las **8 posiciones más frecuentes**.

> **Reflexión previa:** ¿Esperarías que los porteros (GK) sean los más abundantes en el mercado gratuito, o lo serían los defensas? ¿Por qué los clubs podrían retener a jugadores de ciertas posiciones con valor 0$ en lugar de liberarlos?

---

### Pregunta 8 · RDD — Detección de "Joyas Ocultas" Multidimensional (Filtrado Compuesto Estricto)

El peor error en Moneyball es fichar a un jugador que destaca en una sola métrica pero falla en las demás. La directiva exige candidatos que sean sólidos en los **tres atributos clave al mismo tiempo**, no especialistas de una sola dimensión.

Filtre el dataset para encontrar jugadores que cumplan **todas** las siguientes condiciones simultáneamente:

- Transfer Value **"0$"**
- Club válido (no vacío ni "-")
- Visión (Vis) **≥ 14**
- Técnica (Tec) **≥ 14**
- Liderazgo (Ldr) **≥ 14**
- Edad **menor a 28 años**

Muestre el **conteo total** de jugadores que cumplen este perfil y liste los **primeros 10** ordenados por edad de forma **ascendente** (más jóvenes primero). La salida debe incluir: Nombre, Club, Posición, Edad, Vis, Tec y Ldr.

> **Reflexión previa:** ¿Esperas que este filtro sea muy restrictivo o que existan muchos jugadores con estas condiciones? ¿Qué dice sobre el mercado si el resultado es muy pequeño?

---

## BLOQUE B — Análisis Avanzado con Spark SQL

---

### Pregunta 9 · RDD — Radar de Talento Integral – Score Compuesto de Potencial Moneyball

El director técnico exige un solo número que resuma la "ganga" de cada jugador. El analista jefe ha propuesto un Score Compuesto que pondera las tres métricas clave de forma diferente según su importancia táctica:

> **Fórmula del Score Compuesto**
>
> $$\text{Score} = (\text{Vis} \times 0.40) + (\text{Tec} \times 0.35) + (\text{Ldr} \times 0.25)$$
>
> *Lógica: la Visión de juego (40%) aporta más en la construcción táctica; la Técnica (35%) determina la calidad de ejecución; el Liderazgo (25%) asegura cohesión grupal. La suma de pesos es siempre 1.0.*

Usando RDD, filtre únicamente los jugadores con Transfer Value "0$" y calcule este Score para cada uno. Muestre el **Top 10** de jugadores con mayor Score Compuesto. La salida debe incluir: Nombre, Club, Posición y Score (redondeado a 2 decimales), ordenados de mayor a menor score.

> **Pista de exploración:** ¿Qué posición domina el top 10? ¿Los delanteros (ST/CF) o los mediocampistas (M)? ¿Existe algún correlato entre club de origen y score alto? Plantea una hipótesis antes de ejecutar y contrástala con el resultado.

---

### Pregunta 10 · SQL — Análisis de Brecha por Posición – ¿Dónde hay más talento oculto?

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

### Pregunta 11 · SQL — Detección de Monopolios de Talento – Clubes con Reservas Ocultas

La directiva sospecha que algunos clubes acumulan a propósito jugadores con valor 0$ para luego venderlos renovados. Queremos identificar esos "monopolizadores de talento" antes de que lo hagan.

Usando Spark SQL, identifica los clubes que simultáneamente cumplen:

- Tienen **más de 5 jugadores** con Transfer Value "0$"
- Al menos uno de esos jugadores tiene **Liderazgo ≥ 18**
- El **promedio de Técnica** del grupo supera **14**

Para cada club detectado, muestra: nombre del Club, total de jugadores gratuitos, Técnica promedio y el Liderazgo máximo del grupo. Ordena por total de jugadores gratuitos de forma descendente.

> **Pista metodológica:** Puedes utilizar subconsultas o CTE (`WITH ...`) para primero calcular los agregados por club y luego aplicar los filtros de la cláusula `HAVING`. Esto mejora la legibilidad y el rendimiento del plan de ejecución en Spark.

---

### Pregunta 12 · SQL — Segmentación Etaria del Talento por Posición (Análisis de Cohortes)

El departamento financiero quiere minimizar el riesgo de depreciación: un jugador joven con alto potencial puede revalorizarse y venderse con beneficio, mientras que un veterano con valor 0$ representa solo un costo operativo. Para diseñar la política de fichajes, necesitan cruzar la posición con la franja etaria.

Usando Spark SQL sobre el DataFrame de jugadores con Transfer Value **"0$"**, crea una tabla que muestre para cada combinación de **Position** y grupo de edad:

- **"Sub-21"** → Age < 21  
- **"21-25"** → Age entre 21 y 25 inclusive  
- **"26-30"** → Age entre 26 y 30 inclusive  
- **"Veterano"** → Age > 30  

Calcula para cada celda de la tabla: cantidad de jugadores (`COUNT`), Técnica promedio (`AVG Tec`) y Visión promedio (`AVG Vis`). Filtra solo las combinaciones con **al menos 5 jugadores**. Ordena por Position y luego por grupo de edad.

> **Desafío adicional — Responde antes de codificar:**
> 1. ¿En qué franja etaria esperarías encontrar la mayor Técnica promedio y por qué?
> 2. ¿Tiene sentido que un "Veterano" con valor 0$ tenga alta Visión? ¿Qué implicancia táctica tendría ficharlo?
> 3. ¿Cómo usarías esta tabla para construir una plantilla balanceada en experiencia y potencial?

---

### Pregunta 13 · SQL — Ranking de Clubes Exportadores de Talento Subvalorado

La hipótesis del analista jefe es que ciertos clubes funcionan involuntariamente como "academias públicas": forman jugadores con atributos elevados pero los mantienen con valor 0$, posiblemente por razones administrativas o contractuales. Identificar esos clubes nos permite anticiparnos al mercado.

Usando Spark SQL, construye un ranking de clubes según su **"Índice de Talento Subvalorado"**, definido como:

$$\text{Índice} = \text{AVG}(\text{Vis}) + \text{AVG}(\text{Tec}) + \text{AVG}(\text{Ldr})$$

Aplica los siguientes filtros y condiciones:

- Solo jugadores con Transfer Value **"0$"** y club válido
- Solo clubes con **al menos 8 jugadores** en esa condición
- Muestra: nombre del Club, cantidad de jugadores, AVG Vis, AVG Tec, AVG Ldr e Índice (redondeado a 2 decimales)
- Ordena por Índice de forma **descendente** y muestra el **Top 10**

----

## BLOQUE C — Integración y Estrategia

---

### Pregunta 14 · RDD — Índice de Desempeño Relativo por Nacionalidad – ¿Quién está por encima de la media?

No basta saber que un jugador tiene buena técnica en términos absolutos; lo que importa es si supera la media de su propia nacionalidad. Un jugador con Tec = 16 en un grupo cuya media es 13 es más valioso que uno con Tec = 16 en un grupo cuya media es 17.

Filtra y muestra solo los jugadores que están en el **Top 3 de su propia nación** y que además tengan una diferencia positiva respecto a la media. Limita la salida a las **5 naciones con mayor promedio nacional de Técnica** (entre los que cumplen los filtros anteriores).

---

### Pregunta 15 · RDD + SQL — Estrategia de Fichar por Perfil Mixto – Pipeline Completo de Decisión

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

---

### Pregunta 16 · RDD — Análisis de Consistencia por Perfil (Coeficiente de Variación de Atributos)

Un jugador "promedio alto" en un grupo puede estar ocultando una distribución muy desigual: algunos con atributos altísimos y otros muy bajos. Para el scouting es más útil conocer la **consistencia** del grupo que su media. El Coeficiente de Variación (CV) mide exactamente eso: qué tan dispersos están los valores respecto a la media.

Usando RDD, para cada **Posición** presente entre los jugadores con Transfer Value **"0$"**:

1. Calcula la **media** y la **desviación estándar** del atributo Técnica (Tec).
2. Calcula el **Coeficiente de Variación**: CV = (Desviación Estándar / Media) × 100
3. Filtra posiciones con **al menos 15 jugadores**.
4. Muestra los resultados ordenados por CV de forma **ascendente** (menor dispersión primero), con: Posición, cantidad de jugadores, Media Tec, Desviación Estándar Tec y CV (redondeado a 2 decimales).

> **Reflexión estratégica:** Una posición con CV bajo significa que sus jugadores gratuitos son homogéneamente técnicos (o igualmente mediocres). Una con CV alto tiene jugadores muy dispares. ¿Cuál escenario es más favorable para el scouting y por qué? ¿Cómo usarías este análisis para decidir en qué posición vale más la pena invertir tiempo de búsqueda?

---

### Pregunta 17 · RDD + SQL — Pipeline de Fiabilidad — Detección de Candidatos con Perfil Sostenible a Largo Plazo

El consejo directivo no quiere fichajes de impacto inmediato que se deprecien en dos temporadas. Necesitan jugadores cuyo perfil sea sostenible: jóvenes, técnicamente sólidos, con liderazgo suficiente para crecer dentro del vestuario y que no pertenezcan a clubes que ya acumulen demasiado talento gratuito (lo que sugeriría que el jugador tiene problemas de rendimiento no visibles en las métricas).

**Instrucciones del pipeline:**

1. **Con RDD**, filtra jugadores con Transfer Value **"0$"** que cumplan:
   - Edad entre **17 y 24 años** inclusive
   - Tec ≥ **13**, Vis ≥ **12**, Ldr ≥ **11**
   - Club válido (no vacío ni "-")
2. **Calcula el Score de Sostenibilidad** con la siguiente fórmula:

$$\text{Score_Sost} = (\text{Tec} \times 0.45) + (\text{Vis} \times 0.35) + (\text{Ldr} \times 0.20)$$

3. **Convierte el RDD resultante en un DataFrame** y regístralo como vista temporal `candidatos_sostenibles`.
4. **Con Spark SQL**, aplica un segundo filtro: excluye jugadores que pertenezcan a clubes con **más de 20 jugadores** en el mercado gratuito total (usa una subconsulta o CTE sobre el DataFrame completo para calcular ese conteo por club).
5. **Ordena** por Score de Sostenibilidad de mayor a menor y muestra los **primeros 12 candidatos** con: Nombre, Club, Posición, Edad y Score\_Sost (redondeado a 2 decimales).

> **Sección de Reflexión Estratégica — Obligatoria**
>
> **A)** ¿Por qué tiene sentido penalizar a jugadores de clubes con demasiados activos gratuitos? ¿Qué señal de mercado podría estar detrás de ese patrón?
>
> **B)** Compara los pesos del Score de Sostenibilidad con los del Score Compuesto de la Pregunta 7. ¿Qué filosofía de juego refleja cada fórmula?
>
> **C)** ¿Qué tipo de dato adicional del dataset (que no estés usando actualmente) podría enriquecer este pipeline para reducir el riesgo de fichajes fallidos?
>
> **D)** Si el pipeline devuelve menos de 5 candidatos, ¿qué parámetros ajustarías primero y en qué orden? Justifica tu criterio.
