import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import { ApiError } from '../../../lib/api/fetcher';
import { competitionService } from '../api/competitionService';


export const CompetitionLiveAnnouncementAdminPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [draftMessage, setDraftMessage] = useState('');
  const [draftMode, setDraftMode] = useState<'fullscreen' | 'ticker'>('ticker');
  const [editingId, setEditingId] = useState<number | null>(null);
  const announcementsQuery = useQuery({
    queryKey: ['competition-live-announcements-admin', id],
    queryFn: () => competitionService.getLiveAnnouncements(id!),
    enabled: Boolean(id),
    retry: false,
  });
  const configured = competitionService.isLiveAnnouncementAdminAuthConfigured();
  const status = announcementsQuery.error instanceof ApiError
    ? announcementsQuery.error.status : null;
  const announcements = announcementsQuery.data?.announcements ?? [];

  const login = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      await competitionService.createLiveAnnouncementAdminSession(email, password);
      setEmail('');
      void announcementsQuery.refetch();
    } catch (error) {
      if (error instanceof ApiError && [400, 401].includes(error.status)) {
        setMessage('Correo o contraseña incorrectos.');
      } else if (error instanceof ApiError && error.status === 403) {
        setMessage('La cuenta no está habilitada para administración.');
      } else if (error instanceof Error && error.message.includes('no configurada')) {
        setMessage('Autenticación administrativa no configurada.');
      } else {
        setMessage('No pudimos conectar con la administración.');
      }
    } finally {
      setPassword('');
      setBusy(false);
    }
  };

  const logout = async () => {
    setBusy(true);
    setMessage('');
    try {
      await competitionService.deleteLiveAnnouncementAdminSession();
      void announcementsQuery.refetch();
    } catch {
      setMessage('No pudimos cerrar la sesión. Intenta nuevamente.');
    } finally {
      setPassword('');
      setBusy(false);
    }
  };

  const mutate = async (action: () => Promise<unknown>, successMessage: string) => {
    setBusy(true);
    setMessage('');
    try {
      await action();
      setMessage(successMessage);
      void announcementsQuery.refetch();
      return true;
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        await announcementsQuery.refetch();
        setMessage('Otro administrador actualizó los comunicados. Recargamos los datos; revisa e intenta nuevamente.');
      } else if (error instanceof ApiError && error.status === 401) {
        await announcementsQuery.refetch();
        setMessage('La sesión administrativa expiró. Ingresa nuevamente.');
      } else {
        setMessage('No pudimos guardar el cambio. Intenta nuevamente.');
      }
      return false;
    } finally {
      setBusy(false);
    }
  };

  const resetDraft = () => {
    setEditingId(null);
    setDraftMessage('');
    setDraftMode('ticker');
  };
  const draftLimit = draftMode === 'ticker' ? 240 : 1000;

  const submitAnnouncement = async (event: React.FormEvent) => {
    event.preventDefault();
    const current = announcements.find((announcement) => announcement.id === editingId);
    const saved = current
      ? await mutate(() => competitionService.updateLiveAnnouncement(id!, current.id, {
          message: draftMessage.trim(), display_mode: draftMode,
          expected_revision: current.revision,
        }), 'Comunicado actualizado.')
      : await mutate(() => competitionService.createLiveAnnouncement(id!, {
          message: draftMessage.trim(), display_mode: draftMode, expected_revision: 0,
        }), 'Comunicado creado.');
    if (saved) resetDraft();
  };

  const startEditing = (announcement: typeof announcements[number]) => {
    setEditingId(announcement.id);
    setDraftMessage(announcement.message);
    setDraftMode(announcement.display_mode);
    setMessage('');
  };

  const toggleActivation = (announcement: typeof announcements[number]) => mutate(
    () => competitionService.setLiveAnnouncementActivation(id!, announcement.id, {
      is_active: !announcement.is_active,
      expected_revision: announcement.revision,
    }),
    announcement.is_active ? 'Comunicado desactivado.' : 'Comunicado activado.',
  );

  const removeAnnouncement = async (announcement: typeof announcements[number]) => {
    if (!window.confirm('¿Eliminar este comunicado? Esta acción lo quitará del listado.')) return;
    const removed = await mutate(
      () => competitionService.deleteLiveAnnouncement(id!, announcement.id, announcement.revision),
      'Comunicado eliminado.',
    );
    if (removed && editingId === announcement.id) resetDraft();
  };

  if (!configured) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 font-sans"><section className="max-w-lg rounded-3xl bg-white p-8 text-center shadow-xl"><h1 className="text-2xl font-black text-slate-800">Administración no disponible</h1><p className="mt-3 text-slate-600">Autenticación administrativa no configurada.</p></section></main>;
  }

  if (announcementsQuery.isLoading) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 font-sans text-slate-600">Validando sesión administrativa…</main>;
  }

  if (announcementsQuery.isError && status !== 401 && status !== 403) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 font-sans"><section className="max-w-lg rounded-3xl bg-white p-8 text-center shadow-xl"><h1 className="text-2xl font-black text-slate-800">Administración no disponible</h1><p className="mt-3 text-slate-600">No pudimos conectar con la administración.</p><button type="button" className="mt-5 rounded-xl bg-brand-pool px-5 py-3 font-black text-white" onClick={() => announcementsQuery.refetch()}>Reintentar</button></section></main>;
  }

  if (announcementsQuery.isError && status === 403) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 p-6 font-sans"><section className="max-w-lg rounded-3xl bg-white p-8 text-center shadow-xl"><h1 className="text-2xl font-black text-slate-800">Acceso restringido</h1><p className="mt-3 text-slate-600">No tienes permisos para administrar esta competencia.</p><button type="button" disabled={busy} onClick={logout} className="mt-5 rounded-xl border border-slate-300 px-5 py-3 font-black text-slate-700">Cerrar sesión</button></section></main>;
  }

  if (announcementsQuery.isError) {
    return <main className="grid min-h-dvh place-items-center bg-slate-100 p-4 font-sans"><section className="w-full max-w-md overflow-hidden rounded-3xl bg-white shadow-xl"><header className="bg-brand-pool p-7 text-white"><p className="text-xs font-black uppercase tracking-widest text-white/75">SwimStats Chile</p><h1 className="text-2xl font-black">Administrar comunicados</h1></header><form onSubmit={login} className="space-y-4 p-7"><label className="block text-sm font-bold text-slate-700" htmlFor="admin-email">Correo</label><input id="admin-email" type="email" autoComplete="username" required value={email} onChange={(event) => setEmail(event.target.value)} className="w-full rounded-xl border border-slate-300 px-4 py-3" /><label className="block text-sm font-bold text-slate-700" htmlFor="admin-password">Contraseña</label><input id="admin-password" type="password" autoComplete="current-password" required value={password} onChange={(event) => setPassword(event.target.value)} className="w-full rounded-xl border border-slate-300 px-4 py-3" />{message && <p role="alert" className="text-sm font-semibold text-danger">{message}</p>}<button type="submit" disabled={busy} className="w-full rounded-xl bg-brand-pool px-4 py-3 font-black text-white disabled:opacity-50">{busy ? 'Validando…' : 'Ingresar'}</button></form></section></main>;
  }

  return (
    <main className="min-h-dvh bg-slate-100 p-4 font-sans sm:p-8">
      <div className="mx-auto max-w-5xl space-y-5">
        <header className="flex items-center justify-between rounded-2xl bg-white p-5 shadow-sm"><div><p className="text-xs font-black uppercase tracking-widest text-brand-pool">Administración de comunicados</p><h1 className="text-xl font-black text-slate-800">Acceso autorizado</h1></div><button type="button" disabled={busy} onClick={logout} className="rounded-xl border border-slate-300 px-4 py-2 font-bold text-slate-700">Cerrar sesión</button></header>
        <form onSubmit={submitAnnouncement} className="space-y-4 rounded-2xl bg-white p-6 shadow-sm">
          <h2 className="text-lg font-black text-slate-800">{editingId ? 'Editar comunicado' : 'Crear comunicado'}</h2>
          <label htmlFor="announcement-message" className="block text-sm font-bold text-slate-700">Mensaje</label>
          <textarea id="announcement-message" required maxLength={draftLimit} value={draftMessage} onChange={(event) => setDraftMessage(event.target.value)} className="min-h-28 w-full rounded-xl border border-slate-300 px-4 py-3" />
          <p className="text-xs text-slate-500">Máximo {draftLimit} caracteres para este modo.</p>
          <label htmlFor="announcement-mode" className="block text-sm font-bold text-slate-700">Modo de visualización</label>
          <select id="announcement-mode" value={draftMode} onChange={(event) => setDraftMode(event.target.value as 'fullscreen' | 'ticker')} className="w-full rounded-xl border border-slate-300 px-4 py-3"><option value="fullscreen">Pantalla completa</option><option value="ticker">Cinta inferior</option></select>
          <div className="flex flex-wrap gap-3"><button type="submit" disabled={busy || !draftMessage.trim() || draftMessage.length > draftLimit} className="rounded-xl bg-brand-pool px-5 py-3 font-black text-white disabled:opacity-40">{editingId ? 'Guardar cambios' : 'Crear comunicado'}</button>{editingId && <button type="button" disabled={busy} onClick={resetDraft} className="rounded-xl border border-slate-300 px-5 py-3 font-bold text-slate-700">Cancelar edición</button>}</div>
        </form>
        {message && <p role="status" aria-live="polite" className="rounded-xl bg-white px-4 py-3 text-center text-sm font-semibold text-slate-700 shadow-sm">{message}</p>}
        <section className="space-y-3" aria-labelledby="announcement-list-title"><h2 id="announcement-list-title" className="text-lg font-black text-slate-800">Comunicados ({announcements.length})</h2>{announcements.length === 0 ? <p className="rounded-2xl bg-white p-6 text-slate-500 shadow-sm">Aún no hay comunicados.</p> : announcements.map((announcement) => <article key={announcement.id} className="rounded-2xl bg-white p-5 shadow-sm"><div className="flex flex-wrap items-start justify-between gap-3"><div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><span className={`rounded-full px-2 py-1 text-xs font-black ${announcement.is_active ? 'bg-emerald-100 text-emerald-800' : 'bg-slate-100 text-slate-600'}`}>{announcement.is_active ? 'Activo' : 'Inactivo'}</span><span className="text-xs font-bold text-slate-500">{announcement.display_mode === 'fullscreen' ? 'Pantalla completa' : 'Cinta inferior'} · Revisión {announcement.revision}</span></div><p className="mt-3 whitespace-pre-wrap font-semibold text-slate-800">{announcement.message}</p></div><div className="flex flex-wrap gap-2"><button type="button" disabled={busy} onClick={() => startEditing(announcement)} className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-bold">Editar</button><button type="button" disabled={busy} onClick={() => void toggleActivation(announcement)} className="rounded-lg bg-brand-pool px-3 py-2 text-sm font-bold text-white">{announcement.is_active ? 'Desactivar' : 'Activar'}</button><button type="button" disabled={busy} onClick={() => void removeAnnouncement(announcement)} className="rounded-lg border border-red-200 px-3 py-2 text-sm font-bold text-red-700">Eliminar</button></div></div></article>)}</section>
        <Link className="block text-center font-bold text-brand-pool hover:underline" to={`/competitions/${id}/live`}>Ver pantalla pública</Link>
      </div>
    </main>
  );
};
