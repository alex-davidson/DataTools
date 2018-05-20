# TODO

 * **Quirks:** Anything which my spec includes but my code doesn't (aka 'bugs' or 'why doesn't it do what it says it should?')
   * Handle multi-segment variable-length types, both in and out.
     * Handle arbitrary-length binary fields properly, in particular. Currently need to buffer entirely in memory.
 * **Helpful:** Makes life easier for anyone trying to use this.
   * Make the argument parsing better. Possible breaking change.
     * Use something like NDesk's OptionSet.cs, like in ClrSpy. Ideally get permission from work to use the current alpha of Bluewire.Common.Console (after it's not alpha any more).
     * Add options for things which are currently assumed/defaulted. Such as timeouts, and whether to overwrite or append to existing data.
   * Add progress reporting, for very big/slow tables.

Re. segmentable byte streams:
 * Seekable segmentable stream: able to get the length and write it forward-only.
 * Unseekable segmentable stream: reserve length field before writing.
 * Submitting a stream to SQL Server for bulk copy is tricky...

 
+SQL Server data types which I do not plan to support+

(Unless someone can suggest ways of doing so which aren't SQL-Server-specific)

cursor
rowversion
hierarchyid
sql_variant
table

+Performance+

We do seem to spend 10% of our time just aligning reads. That's 20% of the time spent doing CPU things.

Exports are already pretty fast, but we could cut I/O quite a bit by using a memory-mapped file for the intermediary stream when doing compression.
