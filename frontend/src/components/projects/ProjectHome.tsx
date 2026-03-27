export default function ProjectHome() {
  return (
    <div className="flex h-full items-center justify-center p-8">
      <div className="max-w-2xl rounded-[2rem] border border-stone-200 bg-white px-10 py-12 shadow-[0_24px_80px_rgba(51,39,20,0.08)]">
        <div className="text-[11px] font-semibold uppercase tracking-[0.32em] text-stone-400">
          Local Workspace
        </div>
        <h2 className="mt-4 font-serif text-4xl leading-tight text-stone-900">
          Keep every pipeline project in one place.
        </h2>
        <p className="mt-4 text-base leading-7 text-stone-600">
          Open a project from the project browser or create a new one to jump straight into the pipeline editor.
        </p>
        <div className="mt-8 grid gap-3 text-sm text-stone-600 md:grid-cols-2">
          <div className="rounded-2xl bg-stone-50 p-4">
            Project metadata and pipeline definitions now live in a centralized local project catalog.
          </div>
          <div className="rounded-2xl bg-stone-50 p-4">
            Editor changes stay explicit: click Save when you want to persist the current project.
          </div>
        </div>
      </div>
    </div>
  )
}
