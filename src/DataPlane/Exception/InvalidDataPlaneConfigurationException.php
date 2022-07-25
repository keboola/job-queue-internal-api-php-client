<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Exception;

use RuntimeException;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\ConstraintViolationListInterface;

class InvalidDataPlaneConfigurationException extends RuntimeException
{
    private ConstraintViolationListInterface $errors;

    public function __construct(string $dataPlaneId, ConstraintViolationListInterface $errors)
    {
        parent::__construct(sprintf(
            'Data plane "%s" configuration is not valid: %s',
            $dataPlaneId,
            implode("\n", array_map(
                fn (ConstraintViolationInterface $error) =>
                sprintf('%s %s', $error->getPropertyPath(), $error->getMessage()),
                iterator_to_array($errors)
            ))
        ));

        $this->errors = $errors;
    }

    public function getErrors(): ConstraintViolationListInterface
    {
        return $this->errors;
    }
}
