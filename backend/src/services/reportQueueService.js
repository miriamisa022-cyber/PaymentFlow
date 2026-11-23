'use strict';

const {
  startReportWorker,
  setJobProcessing,
  setJobCompleted,
  setJobFailed,
  createQueue,
  closeQueue,
} = require('../queue/reportQueue');
const {
  generateReport,
  generateAccountingCsv,
  reportToCsv,
} = require('./reportService');
const logger = require('../utils/logger');
const { storeReportArtifact } = require('./reportArtifactStorage');

async function processReportJob(job) {
  const { jobId, schoolId, type, startDate, endDate, timezone, schemaVersion, format = 'json' } = job.data;

  await setJobProcessing(jobId);
  logger.info('[ReportQueueService] Processing report job', { jobId, type, schoolId });

  try {
    let artifactKey;
    let contentType;
    let actualSchemaVersion = null;
    if (type === 'accounting_csv') {
      const { csv, schemaVersion: sv } = await generateAccountingCsv({ schoolId, startDate, endDate, timezone });
      await setJobCompleted(jobId, { csv, schemaVersion: sv });
      return { success: true, jobId, type };
    }

    await setJobCompleted(jobId, { artifactKey, schemaVersion: actualSchemaVersion || schemaVersion });
    return { success: true, jobId, type };
  } catch (err) {
    await setJobFailed(jobId, err);
    throw err;
  }
}

async function processStudentExport(job) {
  const { jobId, schoolId } = job.data;
  await setJobProcessing(jobId);
  try {
    const { generateStudentExport } = require('./studentExportService');
    const csv = await generateStudentExport(job.data);
    const artifactKey = `student-exports/${encodeURIComponent(schoolId)}/${jobId}.csv`;
    await storeReportArtifact(artifactKey, csv, 'text/csv', 'attachment; filename="students.csv"');
    await setJobCompleted(jobId, { artifactKey });
  } catch (err) {
    await setJobFailed(jobId, err);
    throw err;
  }
}

let worker = null;

function startWorker() {
  if (worker) return worker;
  createQueue();
  worker = startReportWorker((job) => job.data.type === 'student_export'
    ? processStudentExport(job) : processReportJob(job));
  if (worker) {
    logger.info('[ReportQueueService] Report worker started');
  }
  return worker;
}

async function stopWorker() {
  if (worker) {
    await worker.close();
    worker = null;
  }
  await closeQueue();
}

module.exports = {
  startWorker,
  stopWorker,
};