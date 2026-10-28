import { IsIn, IsOptional, IsString, MinLength } from 'class-validator';

export class CreateActivationDto {
  @IsString()
  @MinLength(1)
  customerId: string;

  @IsString()
  @MinLength(1)
  planId: string;

  /** Permite forzar un fallo desde la UI para demostrar la Saga (RF-01). */
  @IsOptional()
  @IsIn(['none', 'billing', 'provisioning'])
  simulateFailure?: 'none' | 'billing' | 'provisioning';
}
