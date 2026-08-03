import React, { useState } from 'react';

type PasswordFieldProps = {
  id: string;
  label: string;
  value: string;
  onChange: (value: string) => void;
  autoComplete: string;
  required?: boolean;
  minLength?: number;
  disabled?: boolean;
  inputClassName?: string;
};

// `pr-24` reserva el ancho del boton para que el texto no quede debajo de el.
const INPUT_BASE = 'w-full rounded-xl border border-slate-300 py-3 pl-4 pr-24';

/**
 * Campo secreto con opcion de descubrir el contenido, para evitar errores de
 * tipeo a ciegas. Se usa tanto para contraseñas como para el codigo temporal
 * del voluntario.
 *
 * Devuelve un fragmento y no un contenedor a proposito: los formularios que lo
 * usan espacian con `space-y-*` sobre sus hijos directos, asi que envolverlo
 * cambiaria la separacion respecto de los demas campos.
 */
export const PasswordField: React.FC<PasswordFieldProps> = ({
  id, label, value, onChange, autoComplete,
  required, minLength, disabled, inputClassName,
}) => {
  const [revealed, setRevealed] = useState(false);

  return (
    <>
      <label className="block text-sm font-bold text-slate-700" htmlFor={id}>{label}</label>
      <div className="relative">
        <input
          id={id}
          type={revealed ? 'text' : 'password'}
          autoComplete={autoComplete}
          required={required}
          minLength={minLength}
          disabled={disabled}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className={inputClassName ? `${INPUT_BASE} ${inputClassName}` : INPUT_BASE}
        />
        <button
          type="button"
          disabled={disabled}
          onClick={() => setRevealed((current) => !current)}
          // El nombre accesible incorpora la etiqueta del campo para distinguir
          // varios campos secretos en un mismo formulario.
          aria-label={`${revealed ? 'Ocultar' : 'Mostrar'} ${label.toLowerCase()}`}
          aria-controls={id}
          className="absolute inset-y-0 right-0 px-4 text-xs font-black uppercase tracking-wide text-brand-live disabled:opacity-40"
        >
          {revealed ? 'Ocultar' : 'Mostrar'}
        </button>
      </div>
    </>
  );
};
