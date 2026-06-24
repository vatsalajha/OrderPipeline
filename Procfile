api:         uvicorn api.main:app --host 0.0.0.0 --port 8000
restaurant:  uvicorn restaurant.main:app --host 0.0.0.0 --port 8001
courier:     uvicorn courier.main:app --host 0.0.0.0 --port 8002

# NOTE: the worker pool is no longer started by honcho. The API process
# supervises N native worker subprocesses (config: WORKER_COUNT, default 3) so
# that POST /worker/kill can SIGTERM an individual worker for the demo without
# honcho observing a process exit and tearing the whole group down. Restart a
# killed worker live from the dashboard ("+ Worker") or POST /worker/start.
