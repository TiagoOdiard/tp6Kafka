#!/bin/bash
# ============================================================
# Inicializa MongoDB como replica set de un solo nodo.
# Es necesario para poder usar transacciones (fase 4 / outbox).
# ============================================================
set -e

echo "Inicializando replica set rs0 ..."

mongosh --host mongo:27017 --quiet --eval '
try {
  rs.status();
  print("Replica set ya inicializado");
} catch (error) {
  rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongo:27017" }] });
  print("Replica set iniciado");
}
'

echo "Esperando a que mongo sea PRIMARY ..."
for attempt in $(seq 1 40); do
  if mongosh --host mongo:27017 --quiet --eval 'db.hello().isWritablePrimary' | grep -q true; then
    echo "MongoDB PRIMARY listo"
    exit 0
  fi
  sleep 1
done

echo "ERROR: MongoDB no llegó a PRIMARY"
exit 1
