import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { ApiError } from '../../../lib/api/fetcher';
import { competitionService } from '../api/competitionService';

const MIN_PASSWORD_LENGTH = 12;

type RecoveryArrival = {
  accessToken: string;
  tokenHash: string;
  code: string;
  error: string;
};

/** Supabase entrega el resultado del enlace de tres formas segun la version y la
 *  plantilla: token en el fragmento, `token_hash` para canjear, o `code` de
 *  PKCE. Los errores tambien pueden venir en cualquiera de los dos lados. Se
 *  leen ambos para no quedar mostrando el formulario de correo en silencio.
 *  La lectura es pura; borrar la URL es un efecto aparte. Nada se persiste. */
const readRecoveryArrival = (): RecoveryArrival => {
  const fragment = new URLSearchParams(window.location.hash.replace(/^#/, ''));
  const query = new URLSearchParams(window.location.search);
  const pick = (key: string) => fragment.get(key) || query.get(key) || '';
  const isRecovery = pick('type') === 'recovery';
  return {
    accessToken: isRecovery ? pick('access_token') : '',
    tokenHash: isRecovery ? pick('token_hash') : '',
    code: pick('code'),
    error: pick('error_description') || pick('error'),
  };
};

export const AdminPasswordPage: React.FC = () => {
  const [arrival] = useState(readRecoveryArrival);
  const [recoveryToken, setRecoveryToken] = useState(arrival.accessToken);
  const [exchanging, setExchanging] = useState(Boolean(arrival.tokenHash));
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [message, setMessage] = useState('');
  const [done, setDone] = useState(false);
  const [busy, setBusy] = useState(false);
  const configured = competitionService.isLiveAnnouncementAdminAuthConfigured();

  // El token sale de la barra de direcciones apenas se lee, para que no quede a
  // la vista ni viaje si alguien copia el enlace.
  useEffect(() => {
    if (!window.location.hash && !window.location.search) return;
    window.history.replaceState(null, '', window.location.pathname);
  }, []);

  useEffect(() => {
    if (!arrival.tokenHash) return;
    competitionService.verifyAdminRecoveryToken(arrival.tokenHash)
      .then((token) => setRecoveryToken(token))
      .catch(() => setMessage('El enlace expiró o ya fue usado. Solicita uno nuevo.'))
      .finally(() => setExchanging(false));
  }, [arrival]);

  // Diagnostico derivado del arribo: sin esto, un enlace vencido o de un flujo
  // distinto caia en el formulario de correo sin explicar por que.
  const arrivalMessage = arrival.error
    ? `Supabase rechazó el enlace: ${arrival.error}`
    : arrival.code && !arrival.accessToken && !arrival.tokenHash
      ? 'El enlace llegó en formato PKCE, que esta pantalla no puede completar. Pide el enlace desde este formulario en vez del panel de Supabase.'
      : '';
  const visibleMessage = message || arrivalMessage;

  const requestLink = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      await competitionService.requestAdminPasswordRecovery(email);
      setEmail('');
      setDone(true);
      // No se revela si el correo existe: la respuesta es la misma en ambos casos.
      setMessage('Si la cuenta existe, enviamos un enlace para definir la contraseña. Revisa tu correo.');
    } catch (error) {
      setMessage(error instanceof ApiError && error.status === 429
        ? 'Demasiados intentos. Espera unos minutos antes de pedir otro enlace.'
        : 'No pudimos enviar el correo. Intenta nuevamente.');
    } finally {
      setBusy(false);
    }
  };

  const savePassword = async (event: React.FormEvent) => {
    event.preventDefault();
    setMessage('');
    if (password.length < MIN_PASSWORD_LENGTH) {
      setMessage(`La contraseña debe tener al menos ${MIN_PASSWORD_LENGTH} caracteres.`);
      return;
    }
    if (password !== confirmation) {
      setMessage('Las contraseñas no coinciden.');
      return;
    }
    setBusy(true);
    try {
      await competitionService.setAdminPassword(recoveryToken, password);
      setRecoveryToken('');
      setPassword('');
      setConfirmation('');
      setDone(true);
      setMessage('Contraseña actualizada. Ya puedes ingresar con tu correo y contraseña.');
    } catch (error) {
      setMessage(error instanceof ApiError && [401, 403].includes(error.status)
        ? 'El enlace expiró o ya fue usado. Solicita uno nuevo.'
        : 'No pudimos actualizar la contraseña. Intenta nuevamente.');
      setRecoveryToken('');
    } finally {
      setBusy(false);
    }
  };

  if (!configured) {
    return (
      <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 font-sans">
        <section className="max-w-lg rounded-3xl bg-white p-8 text-center shadow-xl">
          <h1 className="text-2xl font-black text-slate-800">Administración no disponible</h1>
          <p className="mt-3 text-slate-600">Autenticación administrativa no configurada.</p>
        </section>
      </main>
    );
  }

  if (exchanging) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 font-sans text-slate-600">Validando el enlace…</main>;
  }

  return (
    <main className="grid min-h-dvh place-items-center bg-slate-100 p-4 font-sans">
      <section className="w-full max-w-md overflow-hidden rounded-3xl bg-white shadow-xl">
        <header className="bg-brand-live p-7 text-white">
          <p className="text-xs font-black uppercase tracking-widest text-white/80">Cuenta administrativa</p>
          <h1 className="text-2xl font-black">{recoveryToken ? 'Definir contraseña' : 'Recuperar acceso'}</h1>
        </header>

        {recoveryToken ? (
          <form onSubmit={savePassword} className="space-y-4 p-7">
            <p className="text-sm text-slate-500">
              Elige una contraseña para poder ingresar sin depender del enlace por correo.
            </p>
            <label className="block text-sm font-bold text-slate-700" htmlFor="new-password">Nueva contraseña</label>
            <input
              id="new-password" type="password" autoComplete="new-password" required
              minLength={MIN_PASSWORD_LENGTH} value={password}
              onChange={(event) => setPassword(event.target.value)}
              className="w-full rounded-xl border border-slate-300 px-4 py-3"
            />
            <label className="block text-sm font-bold text-slate-700" htmlFor="confirm-password">Repetir contraseña</label>
            <input
              id="confirm-password" type="password" autoComplete="new-password" required
              minLength={MIN_PASSWORD_LENGTH} value={confirmation}
              onChange={(event) => setConfirmation(event.target.value)}
              className="w-full rounded-xl border border-slate-300 px-4 py-3"
            />
            <p className="text-xs text-slate-400">Mínimo {MIN_PASSWORD_LENGTH} caracteres.</p>
            {visibleMessage && <p role="alert" aria-live="polite" className="text-sm font-semibold text-slate-700">{visibleMessage}</p>}
            <button type="submit" disabled={busy} className="w-full rounded-xl bg-brand-live px-4 py-3 font-black text-white disabled:opacity-50">
              {busy ? 'Guardando…' : 'Guardar contraseña'}
            </button>
          </form>
        ) : (
          <form onSubmit={requestLink} className="space-y-4 p-7">
            <p className="text-sm text-slate-500">
              Te enviamos un enlace por correo para definir una contraseña. Sirve también si tu
              cuenta se creó con enlace mágico y nunca tuvo una.
            </p>
            <label className="block text-sm font-bold text-slate-700" htmlFor="recovery-email">Correo</label>
            <input
              id="recovery-email" type="email" autoComplete="username" required value={email}
              onChange={(event) => setEmail(event.target.value)}
              className="w-full rounded-xl border border-slate-300 px-4 py-3"
            />
            {visibleMessage && <p role="status" aria-live="polite" className="text-sm font-semibold text-slate-700">{visibleMessage}</p>}
            <button type="submit" disabled={busy} className="w-full rounded-xl bg-brand-live px-4 py-3 font-black text-white disabled:opacity-50">
              {busy ? 'Enviando…' : 'Enviar enlace'}
            </button>
          </form>
        )}

        {done && (
          <p className="border-t border-slate-100 px-7 py-4 text-center text-sm">
            <Link className="font-bold text-brand-live" to="/competitions">Volver a competencias</Link>
          </p>
        )}
      </section>
    </main>
  );
};
