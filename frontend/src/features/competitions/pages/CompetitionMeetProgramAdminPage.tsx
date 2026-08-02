import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import type { MeetProgramPreview } from '../../../lib/schemas/competition';
import { ApiError } from '../../../lib/api/fetcher';
import { competitionService } from '../api/competitionService';

const MAX_PROGRAM_BYTES = 16 * 1024 * 1024;

const detectFormat = (file: File): 'pdf' | 'csv' | null => {
  const name = file.name.toLowerCase();
  if (name.endsWith('.pdf')) return 'pdf';
  if (name.endsWith('.csv')) return 'csv';
  return null;
};

export const CompetitionMeetProgramAdminPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const [file, setFile] = useState<File | null>(null);
  const [scheduledDate, setScheduledDate] = useState('');
  const [preview, setPreview] = useState<MeetProgramPreview | null>(null);
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const configured = competitionService.isLiveAnnouncementAdminAuthConfigured();

  const competitionQuery = useQuery({
    queryKey: ['competition', id],
    queryFn: () => competitionService.getCompetitionDetail(id!),
    enabled: Boolean(id),
  });

  const login = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      await competitionService.createLiveAnnouncementAdminSession(email, password);
      setEmail('');
      setAuthenticated(true);
    } catch (error) {
      if (error instanceof ApiError && [400, 401].includes(error.status)) {
        setMessage('Correo o contraseña incorrectos.');
      } else if (error instanceof ApiError && error.status === 403) {
        setMessage('Tu cuenta no tiene rol de administrador global.');
      } else {
        setMessage('No pudimos validar la sesión. Intenta nuevamente.');
      }
    } finally {
      setPassword('');
      setBusy(false);
    }
  };

  const chooseFile = (selected: File | undefined) => {
    setMessage('');
    setPreview(null);
    if (!selected) { setFile(null); return; }
    if (!detectFormat(selected)) {
      setFile(null);
      setMessage('El sembrado debe ser un archivo .pdf o .csv exportado desde Meet Manager.');
      return;
    }
    if (selected.size > MAX_PROGRAM_BYTES) {
      setFile(null);
      setMessage('El archivo supera el límite de 16 MiB.');
      return;
    }
    setFile(selected);
  };

  const run = async (action: 'preview' | 'publish') => {
    if (!file) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await competitionService.uploadMeetProgram(
        id!, file, detectFormat(file)!, action, scheduledDate || undefined,
      );
      setPreview(result);
      if (result.publication_id) {
        setMessage(result.publication_created
          ? `Sembrado publicado. Publicación ${result.publication_id}.`
          : `El sembrado ya estaba publicado sin cambios (publicación ${result.publication_id}).`);
      } else if (result.state === 'validated') {
        setMessage(action === 'publish'
          ? 'No se pudo publicar.'
          : 'Sembrado validado. Revisa el resumen y confirma la publicación.');
      } else {
        setMessage('El sembrado no pasó las validaciones. No se publicó nada.');
      }
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        setMessage('La sesión administrativa expiró. Ingresa nuevamente.');
      } else if (error instanceof ApiError && error.status === 403) {
        setMessage('Tu cuenta no tiene rol de administrador global.');
      } else {
        setMessage(error instanceof ApiError ? error.message : 'No se pudo procesar el sembrado.');
      }
      setPreview(null);
    } finally {
      setBusy(false);
    }
  };

  const validated = preview?.state === 'validated';
  const published = Boolean(preview?.publication_id);

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

  if (!authenticated) {
    return (
      <main className="grid min-h-dvh place-items-center bg-slate-100 p-4 font-sans">
        <section className="w-full max-w-md overflow-hidden rounded-3xl bg-white shadow-xl">
          <header className="bg-brand-live p-7 text-white">
            <p className="text-xs font-black uppercase tracking-widest text-white/80">Administración global</p>
            <h1 className="text-2xl font-black">Publicar sembrado</h1>
          </header>
          <form onSubmit={login} className="space-y-4 p-7">
            <label className="block text-sm font-bold text-slate-700" htmlFor="program-admin-email">Correo</label>
            <input
              id="program-admin-email" type="email" autoComplete="username" required value={email}
              onChange={(event) => setEmail(event.target.value)}
              className="w-full rounded-xl border border-slate-300 px-4 py-3"
            />
            <label className="block text-sm font-bold text-slate-700" htmlFor="program-admin-password">Contraseña</label>
            <input
              id="program-admin-password" type="password" autoComplete="current-password" required value={password}
              onChange={(event) => setPassword(event.target.value)}
              className="w-full rounded-xl border border-slate-300 px-4 py-3"
            />
            {message && <p role="alert" aria-live="polite" className="text-sm font-semibold text-danger">{message}</p>}
            <button type="submit" disabled={busy} className="w-full rounded-xl bg-brand-live px-4 py-3 font-black text-white disabled:opacity-50">
              {busy ? 'Validando…' : 'Ingresar'}
            </button>
            <p className="text-center text-sm">
              <Link className="font-bold text-brand-live" to="/admin/password">¿Sin contraseña o la olvidaste?</Link>
            </p>
          </form>
        </section>
      </main>
    );
  }

  return (
    <main className="min-h-dvh bg-slate-100 p-4 font-sans sm:p-8">
      <div className="mx-auto max-w-4xl space-y-5">
        <header className="flex flex-wrap items-center justify-between gap-3 rounded-2xl bg-white p-5 shadow-sm">
          <div className="min-w-0">
            <p className="text-xs font-black uppercase tracking-widest text-brand-live">Administración global</p>
            <h1 className="truncate text-xl font-black text-slate-800">Sembrado · {competitionQuery.data?.competition.name ?? `Competencia ${id}`}</h1>
          </div>
          <nav className="flex shrink-0 gap-3 text-xs font-bold">
            <Link className="text-brand-live" to={`/competitions/${id}/live`}>Pantalla pública</Link>
            <Link className="text-brand-live" to={`/competitions/${id}/live/admin`}>Comunicados</Link>
          </nav>
        </header>

        <section className="space-y-4 rounded-2xl bg-white p-6 shadow-sm" aria-labelledby="upload-title">
          <h2 id="upload-title" className="text-lg font-black text-slate-800">Subir sembrado</h2>
          <p className="text-sm text-slate-500">
            Acepta el PDF del programa o el export CSV de HY-TEK Meet Manager. Publicar reemplaza
            el sembrado vigente de esta etapa y piscina; las versiones anteriores se conservan.
          </p>
          <label htmlFor="program-file" className="block text-sm font-bold text-slate-700">Archivo</label>
          <input
            id="program-file" type="file" accept=".pdf,.csv"
            onChange={(event) => chooseFile(event.target.files?.[0])}
            className="block w-full text-sm text-slate-600"
          />
          <label htmlFor="scheduled-date" className="block text-sm font-bold text-slate-700">
            Fecha de la jornada <span className="font-medium text-slate-400">(obligatoria en competencias de varios días)</span>
          </label>
          <input
            id="scheduled-date" type="date" value={scheduledDate}
            onChange={(event) => setScheduledDate(event.target.value)}
            className="rounded-xl border border-slate-300 px-3 py-2 text-sm"
          />
          <div className="flex flex-wrap gap-3 pt-1">
            <button
              type="button" disabled={busy || !file} onClick={() => { void run('preview'); }}
              className="rounded-xl border border-slate-300 px-5 py-3 font-bold text-slate-700 disabled:opacity-40"
            >Validar sin publicar</button>
            <button
              type="button" disabled={busy || !file || !validated || published}
              onClick={() => { void run('publish'); }}
              className="rounded-xl bg-brand-live px-5 py-3 font-black text-white disabled:opacity-40"
            >Publicar sembrado</button>
          </div>
        </section>

        {message && <p role="status" aria-live="polite" className="rounded-xl bg-white px-4 py-3 text-center text-sm font-semibold text-slate-700 shadow-sm">{message}</p>}

        {preview && (
          <section className="space-y-4 rounded-2xl bg-white p-6 shadow-sm" aria-labelledby="summary-title">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <h2 id="summary-title" className="text-lg font-black text-slate-800">Resumen de validación</h2>
              <span className={`rounded-full px-3 py-1 text-xs font-black ${validated ? 'bg-emerald-100 text-emerald-800' : 'bg-amber-100 text-amber-800'}`}>
                {validated ? 'Validado' : 'Requiere revisión'}
              </span>
            </div>

            <dl className="grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
              {([
                ['Inscripciones', preview.counts.entries],
                ['Líneas sin parsear', preview.counts.debug_unparsed_lines],
                ['Parser', preview.source.parser_version],
                ['Origen', preview.source.source_kind],
                ['Competencia en el archivo', preview.source.source_competition_name],
                ['Jornada', preview.source.scheduled_date],
                ['Etapa', preview.source.stage_number],
                ['Piscina', preview.source.pool_role],
              ] as const).map(([label, value]) => (
                <div key={label} className="rounded-xl bg-slate-50 px-3 py-2">
                  <dt className="text-xs font-bold uppercase tracking-wide text-slate-400">{label}</dt>
                  <dd className="truncate font-bold text-slate-700">{value ?? '—'}</dd>
                </div>
              ))}
            </dl>

            <div>
              <h3 className="text-xs font-black uppercase tracking-wider text-slate-500">Eventos detectados ({preview.events.length})</h3>
              <ul className="mt-2 space-y-1 text-sm">
                {preview.events.map(([number, name]) => (
                  <li key={number} className="flex gap-3 rounded-lg bg-slate-50 px-3 py-2">
                    <span className="font-black text-brand-live">#{number}</span>
                    <span className="truncate text-slate-700">{name}</span>
                  </li>
                ))}
              </ul>
            </div>

            {preview.issues.length > 0 && (
              <div>
                <h3 className="text-xs font-black uppercase tracking-wider text-slate-500">Problemas</h3>
                <ul className="mt-2 space-y-2 text-sm">
                  {preview.issues.map((issue) => (
                    <li key={issue.issue_key} className="rounded-lg bg-amber-50 px-3 py-2">
                      <span className="font-black text-amber-900">{issue.severity} · {issue.issue_key}</span>
                      <p className="text-amber-900">{issue.message} ({issue.count})</p>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {preview.unparsed_sample.length > 0 && (
              <div>
                <h3 className="text-xs font-black uppercase tracking-wider text-slate-500">Líneas sin parsear (muestra)</h3>
                <ul className="mt-2 max-h-48 space-y-1 overflow-y-auto text-xs">
                  {preview.unparsed_sample.map((line) => (
                    <li key={`${line.line_number}-${line.reason}`} className="rounded bg-slate-50 px-3 py-2">
                      <span className="font-bold text-slate-500">L{line.line_number} · {line.reason}</span>
                      <p className="truncate font-mono text-slate-600">{line.raw_line}</p>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </section>
        )}
      </div>
    </main>
  );
};
