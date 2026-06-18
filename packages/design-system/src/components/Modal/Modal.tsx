import { useEffect, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

export type ModalSize = 'sm' | 'md' | 'lg'
export type ModalTone = 'default' | 'danger'

export interface ModalProps {
  open: boolean
  onClose: () => void
  title?: ReactNode
  description?: ReactNode
  children?: ReactNode
  /** Right-aligned action row (e.g. Cancel / Confirm). */
  footer?: ReactNode
  size?: ModalSize
  /** `danger` tints the header for destructive/error dialogs. */
  tone?: ModalTone
  closeOnBackdrop?: boolean
  closeOnEsc?: boolean
}

/**
 * Base dialog for the node editor, saved-connection, settings, and error
 * surfaces — one backdrop, escape/backdrop dismissal, and a consistent
 * header/body/footer. Rendered through a portal so it escapes canvas stacking.
 */
export function Modal({
  open,
  onClose,
  title,
  description,
  children,
  footer,
  size = 'md',
  tone = 'default',
  closeOnBackdrop = true,
  closeOnEsc = true,
}: ModalProps) {
  useEffect(() => {
    if (!open || !closeOnEsc) return
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [open, closeOnEsc, onClose])

  if (!open) return null

  return createPortal(
    <div
      className="ds-modal__backdrop"
      onMouseDown={() => {
        if (closeOnBackdrop) onClose()
      }}
    >
      <div
        className={`ds-modal ds-modal--${size} ds-modal--${tone}`}
        role="dialog"
        aria-modal="true"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="ds-modal__header">
          <div className="ds-modal__heading">
            {title != null && <h2 className="ds-modal__title">{title}</h2>}
            {description != null && <p className="ds-modal__desc">{description}</p>}
          </div>
          <button type="button" className="ds-modal__close" aria-label="Close" onClick={onClose}>
            ×
          </button>
        </div>
        {children != null && <div className="ds-modal__body">{children}</div>}
        {footer != null && <div className="ds-modal__footer">{footer}</div>}
      </div>
    </div>,
    document.body,
  )
}
