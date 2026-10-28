import { Body, Controller, Get, HttpCode, Param, Post } from '@nestjs/common';
import { ActivationService } from './activation.service';
import { CreateActivationDto } from '../dto/create-activation.dto';

@Controller('activations')
export class ActivationController {
  constructor(private readonly activationService: ActivationService) {}

  /**
   * RF-01 / RF-02: responde 202 Accepted de inmediato, sin esperar
   * a billing ni provisioning.
   */
  @Post()
  @HttpCode(202)
  create(@Body() dto: CreateActivationDto) {
    return this.activationService.create(dto);
  }

  @Get()
  findAll() {
    return this.activationService.findAll();
  }

  /** RF-09: estado actual + historial de la activación. */
  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.activationService.findOne(id);
  }
}
