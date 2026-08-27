import { Learner } from './Learner.js';
import { QuizResult } from './QuizResult.js';

export type FabricQuizSchema = {
  Learner: Learner;
  QuizResult: QuizResult;
};

export const schema = [Learner, QuizResult];
