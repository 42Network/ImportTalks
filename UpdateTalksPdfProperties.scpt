//@osa-lang:JavaScript
/* DEVONthink JavaScript to update General Conference PDF file properties
   PDF Author = Custom metadata Speaker (ID 'mdspeaker')
   PDF Subject = Custom metadata Conference (ID 'mdconference') formatted as "<Year> <Month> General Conference" E.g. "1971 April General Conference"

   Nathan Ellsworth - August 2024

   Extensive help taken from chrillek and cgrunenberg from DEVONthink forums:

   https://discourse.devontechnologies.com/t/javascript-get-custom-metadata-to-rename-file/68445/24
   https://discourse.devontechnologies.com/t/tag-name-into-custom-metadata/70050/3
   https://discourse.devontechnologies.com/t/custom-metadata-import/77900/7
*/
ObjC.import('PDFKit');

function performsmartrule(records) {
   const app = Application("DEVONthink 3");
   app.includeStandardAdditions=true;

   records.forEach (r => {

		const m = r.customMetaData();
  		const conf = m['mdconference'];
  		const spk  = m['mdspeaker'];
		const sess = m['mdsession'];
		const conf_year = conf.split(" ")[1]
		const conf_month = conf.split(" ")[0]
		const subject = conf_year + " " + conf_month + " General Conference"
  		/* app.displayDialog(conf); */
  		/* const conf = app.getCustomMetadata({for:"mdconference", from:r, defaultValue:""}); */
  		/* convert record's path to NSURL object */
  		const docURL = $.NSURL.fileURLWithPath($(r.path()));
  		/* app.displayDialog(r.path()); */
  		/* load the PDF document from this URL */
  		const PDFDoc = $.PDFDocument.alloc.initWithURL(docURL);
  		/* get the current PDF attributes as a MUTABLE dictionary.
     		other dictionaries can't be modified! */
  		const PDFAttributes = $.NSMutableDictionary.dictionaryWithDictionary(PDFDoc.documentAttributes);
  		/* Set the PDF properties */
  		PDFAttributes.setObjectForKey(subject, $("Subject"));
  		PDFAttributes.setObjectForKey(spk, $("Author"));
  		/* Update the PDF attributes */
  		PDFDoc.documentAttributes = $(PDFAttributes);
  		/* Write the PDF doc back to the URL */
  		const result = PDFDoc.writeToURL(docURL);

	})
}

(() => {
if (currentAppID() === "DNtp") return;
const app = Application("DEVONthink 3");
performsmartrule(app.selectedRecords());
})()

function currentAppID() {
  const p = Application.currentApplication().properties();
  return Application(p.name).id();
}