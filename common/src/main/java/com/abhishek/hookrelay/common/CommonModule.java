package com.abhishek.hookrelay.common;

/**
 * Marker for component scanning.
 *
 * <p>Both applications scan {@code scanBasePackageClasses = {..., CommonModule.class}} so every
 * Spring bean in this module is found, rather than enumerating sub-packages one at a time — which
 * silently drops a whole package the moment a new one is added, with a
 * {@code NoSuchBeanDefinitionException} at startup as the only clue.
 */
public interface CommonModule {
}
