# Sistemas Distribuidos Agent-Platform

## Autores

| **Nombre(s) y Apellidos**    |            **Correo**            | **Grupo** |
| :-----------------------:    | :------------------------------: | :-------: |
|  Juan Carlos Esquivel Lamis  |    jesquivel960729@gmail.com     |   C-511   |
|  Juan Carlos Vázquez García  |    vazquezjc097@gmail.com        |   C-512   |
|  Yandy Sánchez Orosa         |    skullcandy961205@gmail.com    |   C-511   |

## Descripción

El proyecto de Sistemas Distribuidos consiste en el diseño, implementación, evaluación y análisis de una Plataforma de Agentes. El sistema desarrollado comprende todas las funcionalidades de una Plataforma de Agentes, desde la identificación, registro y localización de agentes, así como un servicio de búsqueda de agentes dependiendo de sus funcionalidades, tolerancia a fallas, y la posible corrida de la plataforma independientemente de los agentes que participaron en la corrida anterior. Para la realización del proyecto nos apoyamos en el modelo FIPA y el protocolo Chord.

## Ejecución

Para ejecutar nuestro proyecto es necesario escribir las siguientes líneas desde una terminal abierta en esta misma dirección:

```bash
python Node.py [ip] [port]
```

## Restricciones al correr el proyecto 

- Utilizamos una función de Hash para obtener un Id del nodo a partir de su ip, puerto y servicio que realiza, por lo que sólo permitimos mil (1000) servicios diferentes y nodos por servicios.
- El *primer* nodo de la red necesita tener su *agent.py* en la carpeta **Agent** del directorio del proyecto.


