import { FormEvent, useEffect, useMemo, useState } from 'react';
import { io, Socket } from 'socket.io-client';

// En el navegador, la API corre en el mismo host que la UI.
const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

type Step = { status: 'OK' | 'FAILED'; at: string; reason?: string };

interface HistoryEvent {
  eventId: string;
  eventType: string;
  source: string;
  at: string;
  payload?: any;
}

interface Activation {
  _id: string;
  customerId: string;
  planId: string;
  simulateFailure: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
  steps?: { billing?: Step; provisioning?: Step };
  history?: HistoryEvent[];
  createdAt?: string;
}

interface WsEvent {
  activationId: string;
  status: Activation['status'];
  customerId: string;
  planId: string;
  event: {
    eventId: string;
    eventType: string;
    source: string;
    occurredAt: string;
    payload?: any;
  };
  steps?: Activation['steps'];
}

const EVENT_LABEL: Record<string, string> = {
  ActivationRequested: 'Activación solicitada',
  BillingAccountCreated: 'Cuenta de facturación creada',
  BillingFailed: 'Facturación rechazada',
  BillingAccountCancelled: 'Cuenta anulada (compensación)',
  ProvisioningCompleted: 'Aprovisionamiento completado',
  ProvisioningFailed: 'Aprovisionamiento fallido',
  ActivationCompleted: 'Activación completada',
  ActivationFailed: 'Activación fallida',
};

const STATUS_LABEL: Record<string, string> = {
  PENDING: 'Pendiente',
  IN_PROGRESS: 'En progreso',
  ACTIVE: 'Activa',
  FAILED: 'Fallida',
};

function eventDetail(event: HistoryEvent): string {
  const payload = event.payload ?? {};
  if (payload.reason) return payload.reason;
  if (payload.accountId) {
    return `Cuenta ${payload.accountId}${
      payload.amount ? ` · $${payload.amount}` : ''
    }`;
  }
  if (payload.serviceDetails) return payload.serviceDetails;
  if (payload.planId) return `Plan ${payload.planId}`;
  return '';
}

function formatTime(value?: string): string {
  if (!value) return '';
  return new Date(value).toLocaleTimeString('es-AR', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

export default function App() {
  const [activations, setActivations] = useState<Activation[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<Activation | null>(null);
  const [events, setEvents] = useState<Record<string, HistoryEvent[]>>({});
  const [connected, setConnected] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [toast, setToast] = useState<string>('');
  const [error, setError] = useState<string>('');

  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState('PLAN DIGITAL');
  const [simulateFailure, setSimulateFailure] = useState<'none' | 'billing' | 'provisioning'>(
    'none',
  );

  // ---------------------------------------------------------
  // Carga inicial: últimas activaciones
  // ---------------------------------------------------------
  useEffect(() => {
    fetch(`${API_URL}/activations`)
      .then((res) => res.json())
      .then((data: Activation[]) => {
        setActivations(data);
        if (data.length > 0) {
          void openActivation(data[0]._id);
        }
      })
      .catch(() => setError('No se pudo conectar con activation-api (http://localhost:3000).'));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ---------------------------------------------------------
  // WebSocket: timeline en vivo (RF-08)
  // ---------------------------------------------------------
  useEffect(() => {
    const socket: Socket = io(API_URL, { transports: ['websocket', 'polling'] });

    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));

    socket.on('activation.event', (message: WsEvent) => {
      const incoming: HistoryEvent = {
        eventId: message.event.eventId,
        eventType: message.event.eventType,
        source: message.event.source,
        at: message.event.occurredAt,
        payload: message.event.payload,
      };

      setEvents((previous) => {
        const list = previous[message.activationId] ?? [];
        if (list.some((event) => event.eventId === incoming.eventId)) {
          return previous;
        }
        return { ...previous, [message.activationId]: [...list, incoming] };
      });

      setActivations((previous) => {
        const existing = previous.find((item) => item._id === message.activationId);
        if (!existing) {
          return [
            {
              _id: message.activationId,
              customerId: message.customerId,
              planId: message.planId,
              simulateFailure: 'none',
              status: message.status,
              steps: message.steps,
            },
            ...previous,
          ];
        }
        return previous.map((item) =>
          item._id === message.activationId
            ? { ...item, status: message.status, steps: message.steps ?? item.steps }
            : item,
        );
      });

      setDetail((previous) =>
        previous && previous._id === message.activationId
          ? { ...previous, status: message.status, steps: message.steps ?? previous.steps }
          : previous,
      );
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  async function openActivation(id: string): Promise<void> {
    setSelectedId(id);
    try {
      const response = await fetch(`${API_URL}/activations/${id}`);
      if (!response.ok) throw new Error('No encontrada');
      const data: Activation = await response.json();
      setDetail(data);

      setEvents((previous) => {
        const buffered = previous[id] ?? [];
        const merged = [...(data.history ?? []).map((entry) => ({ ...entry })), ...buffered];
        const unique = new Map<string, HistoryEvent>();
        merged.forEach((entry) => unique.set(entry.eventId, entry));
        return { ...previous, [id]: [...unique.values()] };
      });
    } catch {
      setError(`No se pudo cargar la activación ${id}.`);
    }
  }

  async function handleSubmit(event: FormEvent): Promise<void> {
    event.preventDefault();
    setSubmitting(true);
    setError('');
    setToast('');

    try {
      const response = await fetch(`${API_URL}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, planId, simulateFailure }),
      });

      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.message ?? `Error HTTP ${response.status}`);
      }

      const data = await response.json();
      setToast(`Activación ${data.activationId} creada (202 Accepted)`);
      await openActivation(data.activationId);
    } catch (err: any) {
      setError(err.message ?? 'No se pudo crear la activación.');
    } finally {
      setSubmitting(false);
    }
  }

  const timeline = useMemo(() => {
    if (!selectedId) return [];
    return [...(events[selectedId] ?? [])].sort(
      (a, b) => new Date(a.at).getTime() - new Date(b.at).getTime(),
    );
  }, [events, selectedId]);

  const counters = useMemo(
    () => ({
      total: activations.length,
      active: activations.filter((item) => item.status === 'ACTIVE').length,
      failed: activations.filter((item) => item.status === 'FAILED').length,
    }),
    [activations],
  );

  return (
    <div className="page">
      <header className="topbar">
        <div>
          <div className="eyebrow">SISTEMAS DISTRIBUIDOS · 2026</div>
          <h1>Activación de Servicios con Kafka</h1>
          <p className="subtitle">
            POC de una <b>Saga coreografiada</b>: un evento dispara facturación,
            aprovisionamiento, notificación y CRM.
          </p>
        </div>
        <div className="topbar-right">
          <span className={`live ${connected ? 'on' : 'off'}`}>
            <span className="dot" />
            {connected ? 'WebSocket conectado' : 'Sin conexión'}
          </span>
          <div className="links">
            <a href="http://localhost:8080" target="_blank" rel="noreferrer">
              Kafka UI
            </a>
            <a href="http://localhost:8025" target="_blank" rel="noreferrer">
              Mailhog
            </a>
          </div>
        </div>
      </header>

      <main className="layout">
        {/* ---------------- Columna izquierda ---------------- */}
        <section className="column">
          <div className="card">
            <h2>Contratar plan</h2>
            <p className="hint">
              El POST responde <code>202 Accepted</code> al instante, sin esperar a
              los demás servicios.
            </p>

            <form onSubmit={handleSubmit} className="form">
              <label>
                Cliente
                <input
                  value={customerId}
                  onChange={(event) => setCustomerId(event.target.value)}
                  placeholder="C-1234"
                  required
                />
              </label>

              <label>
                Plan
                <select value={planId} onChange={(event) => setPlanId(event.target.value)}>
                  <option>PLAN BASICO</option>
                  <option>PLAN DIGITAL</option>
                  <option>PLAN PREMIUM</option>
                </select>
              </label>

              <label>
                Fallo simulado
                <select
                  value={simulateFailure}
                  onChange={(event) =>
                    setSimulateFailure(event.target.value as typeof simulateFailure)
                  }
                >
                  <option value="none">Sin fallo (camino feliz)</option>
                  <option value="billing">Fallo en billing</option>
                  <option value="provisioning">Fallo en provisioning</option>
                </select>
              </label>

              <button type="submit" disabled={submitting}>
                {submitting ? 'Publicando evento…' : 'Contratar plan'}
              </button>
            </form>

            {toast && <div className="alert ok">{toast}</div>}
            {error && <div className="alert error">{error}</div>}
          </div>

          <div className="card">
            <div className="card-title">
              <h2>Activaciones</h2>
              <div className="mini-stats">
                <span>{counters.total} total</span>
                <span className="ok-text">{counters.active} activas</span>
                <span className="error-text">{counters.failed} fallidas</span>
              </div>
            </div>

            <div className="activation-list">
              {activations.length === 0 && (
                <p className="empty">Todavía no hay activaciones.</p>
              )}
              {activations.map((activation) => (
                <button
                  key={activation._id}
                  type="button"
                  className={`activation-item ${
                    activation._id === selectedId ? 'selected' : ''
                  }`}
                  onClick={() => openActivation(activation._id)}
                >
                  <div className="activation-main">
                    <span className="mono">{activation._id}</span>
                    <span className={`badge ${activation.status.toLowerCase()}`}>
                      {STATUS_LABEL[activation.status] ?? activation.status}
                    </span>
                  </div>
                  <div className="activation-meta">
                    {activation.customerId} · {activation.planId}
                  </div>
                </button>
              ))}
            </div>
          </div>
        </section>

        {/* ---------------- Columna derecha ---------------- */}
        <section className="column">
          <div className="card detail">
            {!detail && (
              <div className="empty-state">
                <h2>Seleccioná una activación</h2>
                <p>La timeline se actualiza en vivo por WebSocket.</p>
              </div>
            )}

            {detail && (
              <>
                <div className="detail-head">
                  <div>
                    <div className="eyebrow">ACTIVACIÓN</div>
                    <h2 className="mono">{detail._id}</h2>
                    <p className="hint">
                      Cliente <b>{detail.customerId}</b> · Plan <b>{detail.planId}</b>
                      {detail.simulateFailure !== 'none' && (
                        <>
                          {' '}
                          · fallo simulado en <b>{detail.simulateFailure}</b>
                        </>
                      )}
                    </p>
                  </div>
                  <span className={`badge big ${detail.status.toLowerCase()}`}>
                    {STATUS_LABEL[detail.status] ?? detail.status}
                  </span>
                </div>

                <div className="steps">
                  <StepChip name="Facturación" step={detail.steps?.billing} />
                  <StepChip name="Aprovisionamiento" step={detail.steps?.provisioning} />
                </div>

                <h3>Timeline de eventos</h3>
                <ul className="timeline">
                  {timeline.map((event) => (
                    <li key={event.eventId} className={eventClass(event.eventType)}>
                      <span className="marker" />
                      <div className="event-body">
                        <div className="event-head">
                          <strong>{EVENT_LABEL[event.eventType] ?? event.eventType}</strong>
                          <span className="time">{formatTime(event.at)}</span>
                        </div>
                        <div className="event-meta">
                          <span className="tag">{event.source}</span>
                          <span className="event-detail">{eventDetail(event)}</span>
                        </div>
                      </div>
                    </li>
                  ))}
                  {timeline.length === 0 && (
                    <p className="empty">Esperando eventos…</p>
                  )}
                </ul>
              </>
            )}
          </div>
        </section>
      </main>

      <footer>
        Kafka · KRaft · NestJS · React · MongoDB · Mailhog — POC académica
      </footer>
    </div>
  );
}

function StepChip({ name, step }: { name: string; step?: Step }) {
  const state = step?.status ?? 'PENDING';
  return (
    <div className={`step-chip ${state.toLowerCase()}`}>
      <div className="step-name">{name}</div>
      <div className="step-status">
        {state === 'OK' ? 'Completado' : state === 'FAILED' ? 'Falló' : 'Pendiente'}
      </div>
      {step?.reason && <div className="step-reason">{step.reason}</div>}
    </div>
  );
}

function eventClass(eventType: string): string {
  if (
    eventType === 'BillingFailed' ||
    eventType === 'ProvisioningFailed' ||
    eventType === 'ActivationFailed' ||
    eventType === 'BillingAccountCancelled'
  ) {
    return 'event failed';
  }
  if (eventType === 'ActivationCompleted' || eventType === 'ProvisioningCompleted') {
    return 'event success';
  }
  return 'event neutral';
}
