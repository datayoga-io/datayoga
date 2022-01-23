import { Step } from "./step";
/**
 * representation of an ETL job
 */
export interface Job {
  /**
   * unique identifier for this job
   */
  id: string;
  /**
   * Description of the job
   */
  runs_on: string;
  /**
   * Description of the job
   */
  description?: string;
  /**
   * date created
   */
  created_at?: string;
  /**
   * Date updated
   */
  updated_at?: string;
  /**
   * List of steps. These will be run in order, where each step's output is the next step's input, unless the 'inputs' clause is specified
   */
  steps: Step[];
  [k: string]: unknown;
}
